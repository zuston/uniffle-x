use std::borrow::BorrowMut;
use std::fs::read;
use std::io::Read;
use std::ops::Deref;
use std::sync::{Arc, mpsc, Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::atomic::Ordering::AcqRel;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use anyhow::{Result, anyhow};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::mapref::one::{Ref, RefMut};
use roaring::{RoaringTreemap, treemap};
use tokio::runtime::Runtime;
use tonic::codegen::ok;
use tracing::callsite::register;
use crate::config::Config;
use crate::error::DatanodeError;
use crate::proto::uniffle::ShuffleData;
use crate::store;
use crate::store::{PartitionedData, PartitionedDataBlock, ResponseData, ResponseDataIndex, Store, StoreProvider};
use crate::store::hybrid::HybridStore;
use crate::store::memory::MemoryStore;

#[derive(Debug, Clone)]
enum DataDistribution {
    NORMAL,
    LOCAL_ORDER
}

pub const MAX_CONCURRENCY_PER_PARTITION_TO_WRITE: i32 = 20;

#[derive(Debug, Clone)]
struct AppConfigOptions {
    data_distribution: DataDistribution,
    max_concurrency_per_partition_to_write: i32
}

// =============================================================

#[derive(Clone)]
pub struct App {
    app: Arc<AppInner>,
    store: Arc<HybridStore>,
    bitmap_of_blocks: DashMap<i32, DashMap<i32, Arc<RwLock<RoaringTreemap>>>>
}

impl App {
    fn from(app_id: String, config_options: Option<AppConfigOptions>, store: Arc<HybridStore>) -> Self {
        App {
            app: Arc::new(
                AppInner {
                    app_id,
                    partitions: DashMap::new(),
                    app_config_options: config_options,
                    latest_heartbeat_time: AtomicU64::new(App::current_timestamp())
                },
            ),
            store,
            bitmap_of_blocks: DashMap::new()
        }
    }

    fn current_timestamp() -> u64 {
        let current_time = SystemTime::now();
        let timestamp = current_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        timestamp
    }

    fn get_latest_heartbeat_time(&self) -> u64 {
        self.app.latest_heartbeat_time.load(Ordering::SeqCst)
    }

    pub fn heartbeat(&mut self) -> Result<()> {
        let timestamp = App::current_timestamp();
        self.app.latest_heartbeat_time.swap(timestamp, Ordering::SeqCst);
        Ok(())
    }

    pub fn register_shuffle(&self, shuffle_id: i32) -> Result<()> {
        self.app.partitions.entry(shuffle_id).or_insert_with(||vec![]);
        Ok(())
    }

    pub async fn insert(&self, ctx: WritingViewContext) -> Result<()> {
        self.store.insert(ctx).await
    }

    pub async fn select(&self, ctx: ReadingViewContext) -> Result<ResponseData> {
        self.store.get(ctx).await
    }

    pub async fn list_index(&self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex> {
        self.store.get_index(ctx).await
    }

    pub async fn require_buffer(&self, ctx: RequireBufferContext) -> Result<(bool, i64)> {
        self.store.require_buffer(ctx).await
    }

    fn get_underlying_partition_bitmap(&self, uid: PartitionedUId) -> Arc<RwLock<RoaringTreemap>> {
        let shuffle_id = uid.shuffle_id;
        let partition_id = uid.partition_id;
        let shuffle_entry = self.bitmap_of_blocks.entry(shuffle_id).or_insert_with(||DashMap::new());
        let mut partition_bitmap = shuffle_entry.entry(partition_id).or_insert_with(||Arc::new(RwLock::new(RoaringTreemap::new())));

        partition_bitmap.clone()
    }

    pub async fn get_block_ids(&self, ctx: GetBlocksContext) -> Result<Bytes> {
        let mut partition_bitmap = self.get_underlying_partition_bitmap(ctx.uid);
        let partition_bitmap = partition_bitmap.read().unwrap();

        let size = partition_bitmap.serialized_size();
        let mut std_bytes = Vec::with_capacity(size);
        partition_bitmap.serialize_into(&mut std_bytes).unwrap();

        Ok(
            Bytes::from(std_bytes)
        )
    }

    pub async fn report_block_ids(&'static self, ctx: ReportBlocksContext) -> Result<()> {
        let mut partition_bitmap_wrapper = self.get_underlying_partition_bitmap(ctx.uid);
        let mut partition_bitmap = partition_bitmap_wrapper.write().unwrap();

        for block_id in ctx.blocks {
            partition_bitmap.insert(block_id as u64);
        }

        Ok(())
    }

    pub async fn purge(&self, app_id: String, shuffle_id: Option<i32>) -> Result<()> {
        Ok(())
    }
}

pub struct ReportBlocksContext {
    uid: PartitionedUId,
    blocks: Vec<i64>
}

pub struct GetBlocksContext {
    uid: PartitionedUId,
    data: Bytes
}

#[derive(Debug)]
pub struct AppInner {
    app_id: String,
    // key: shuffleId, value: partitionIds
    partitions: DashMap<i32, Vec<i32>>,
    app_config_options: Option<AppConfigOptions>,
    latest_heartbeat_time: AtomicU64
}

#[derive(Debug, Clone)]
pub struct WritingViewContext {
    pub uid: PartitionedUId,
    pub data_blocks: Vec<PartitionedDataBlock>
}

pub struct ReadingViewContext {
    pub uid: PartitionedUId,
    pub reading_options: ReadingOptions
}

pub struct ReadingIndexViewContext {
    pub partition_id: PartitionedUId
}

pub struct RequireBufferContext {
    pub uid: PartitionedUId,
    pub size: i64
}

pub enum ReadingOptions {
    MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(i64, i64),
    FILE_OFFSET_AND_LEN(i64, i64)
}

// ==========================================================

#[derive(Debug, Clone)]
pub enum PurgeEvent {
    // app_id
    HEART_BEAT_TIMEOUT(String),
    // app_id + shuffle_id
    APP_PARTIAL_SHUFFLES_PURGE(String, Vec<i32>),
    // app_id
    APP_PURGE(String)
}

pub type AppManagerRef = Arc<AppManager>;

pub struct AppManager {
    apps: DashMap<String, App>,
    receiver: crossbeam_channel::Receiver<PurgeEvent>,
    sender: crossbeam_channel::Sender<PurgeEvent>,
    store: Arc<HybridStore>
}

impl AppManager {
    fn new(config: Config) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let manager = AppManager {
            apps: DashMap::new(),
            receiver,
            sender,
            store: Arc::new(
                StoreProvider::get(config)
            )
        };
        manager
    }
}

impl AppManager {
    pub(crate) fn get_ref(config: Config) -> AppManagerRef {
        let app_ref = Arc::new(AppManager::new(config));

        let app_manager_ref_cloned = app_ref.clone();

        tokio::spawn(async move {
            // todo: accept the purge event to clear the unused data.
            // todo: purge the app if it's timeout
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                for item in app_manager_ref_cloned.apps.iter() {
                    let (key, app) = item.pair();
                    let last_time = app.get_latest_heartbeat_time();
                }
            }
        });
        app_ref
    }

    pub(crate) fn get_app(&self, app_id: &str) -> Option<App> {
        self.apps.get(app_id).map(|v| v.value().clone())
    }

    pub fn register(&self, app_id: String, shuffle_id: i32) -> Result<()> {
        let mut appRef = self.apps.entry(app_id.clone()).or_insert_with(|| App::from(app_id, None, self.store.clone()));
        appRef.register_shuffle(shuffle_id)
    }

    pub fn unregister(&self, app_id: String, shuffle_ids: Option<Vec<i32>>) -> Result<(), DatanodeError> {
        match shuffle_ids {
            Some(ids) => {
                let res = self.sender.send(PurgeEvent::APP_PARTIAL_SHUFFLES_PURGE(app_id, ids))?;
                Ok(res)
            },
            _ => {
                let res = self.sender.send(PurgeEvent::APP_PURGE(app_id))?;
                Ok(res)
            }
        }
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Default, Debug, Hash, Clone)]
pub struct PartitionedUId {
    pub app_id: String,
    pub shuffle_id: i32,
    pub partition_id: i32
}

impl PartitionedUId {
    pub(crate) fn from(app_id: String, shuffle_id: i32, partition_id: i32) -> PartitionedUId {
        PartitionedUId {
            app_id,
            shuffle_id,
            partition_id
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use dashmap::DashMap;
    use crate::app::{AppManager, PartitionedUId, ReadingOptions, ReadingViewContext, WritingViewContext};
    use crate::Config;
    use crate::config::{HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig};
    use crate::store::{PartitionedDataBlock, ResponseData};
    use crate::store::hybrid::HybridStore;

    fn mock_config() -> Config {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let config = Config {
            memory_store: Some(MemoryStoreConfig {
                capacity: 1024 * 1024
            }),
            localfile_store: Some(LocalfileStoreConfig {
                data_paths: vec![temp_path]
            }),
            hybrid_store: Some(HybridStoreConfig {
                memory_spill_high_watermark: 0.8,
                memory_spill_low_watermark: 0.7
            }),
            store_type: None,
            grpc_port: None
        };
        config
    }

    #[tokio::test]
    async fn app_put_get_purge_test() {
        let app_id = "app_put_get_purge_test-----id";

        let appManagerRef = AppManager::get_ref(mock_config()).clone();
        appManagerRef.register(app_id.clone().into(), 1).unwrap();

        if let Some(mut app) = appManagerRef.get_app("app_id".into()) {
            let writingCtx = WritingViewContext {
                uid: PartitionedUId {
                    app_id: app_id.clone().into(),
                    shuffle_id: 1,
                    partition_id: 0
                },
                data_blocks: vec![
                    PartitionedDataBlock {
                        block_id: 0,
                        length: 10,
                        uncompress_length: 20,
                        crc: 10,
                        data: Default::default(),
                        task_attempt_id: 0
                    },
                    PartitionedDataBlock {
                        block_id: 1,
                        length: 20,
                        uncompress_length: 30,
                        crc: 0,
                        data: Default::default(),
                        task_attempt_id: 0
                    }
                ]
            };
            let result = app.insert(writingCtx);
            if result.await.is_err() {
                panic!()
            }

            let readingCtx = ReadingViewContext {
                uid: Default::default(),
                reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000)
            };

            // let result = app.select(readingCtx);
            // if result.await.is_err() {
            //     panic!()
            // }
            //
            // match result.await.unwrap() {
            //     ResponseData::mem(data) => {
            //         assert_eq!(2, data.shuffle_data_block_segments.len());
            //     },
            //     _ => todo!()
            // }
        }
    }

    #[tokio::test]
    async fn app_manager_test() {
        let appManagerRef = AppManager::get_ref(mock_config()).clone();
        appManagerRef.register("app_id".into(), 1).unwrap();
        if let Some(app) = appManagerRef.get_app("app_id".into()) {
            assert_eq!("app_id", app.app.app_id);
        }
    }

    #[tokio::test]
    async fn test_async_io() {
        fn test_write() {

        }
    }

    #[tokio::test]
    async fn test_deref() {
        struct Person {
            age: i32
        }

        #[derive(Clone)]
        struct PersonRef {
            person: Arc<Person>
        }

        let personRef = PersonRef {
            person: Arc::new(
                Person {
                    age: 12
                }
            )
        };

        let c1 = personRef.clone();
        let f1 = tokio::spawn(async move {
            println!("{}", c1.person.age);
        });

        f1.await;
        println!("{}", personRef.person.age);
    }

    #[test]
    fn test_dashmap_change_value() {
        let mapper = DashMap::new();
        mapper.insert("1", 1);

        // Increment the value associated with the key "1" by 2
        if let Some(mut entry) = mapper.get_mut("1") {
            *entry += 2;
        }

        // Get the modified value
        let v1 = mapper.get("1");
        if let Some(value) = v1 {
            println!("Modified value: {:#?}", value.value());
        } else {
            println!("Key not found");
        }
    }

    #[tokio::test]
    async fn test_hashmap_ref() {
        struct Person {
            age:  i32
        }

        impl Person {
            fn set_age(&mut self, i : i32) {
                self.age= i;
            }
        }
        let dashmap = Arc::new(DashMap::new());
        dashmap.insert(1, Person {age: 100});
        dashmap.insert(2, Person {age: 200});

        let c1 = dashmap.clone();
        let future = tokio::spawn(async move {
            let optionCal = c1.get_mut(&1);
            let mut val = optionCal.unwrap();
            val.set_age(10);

            println!("Done0")

        });

        let c2 = dashmap.clone();
        let future1 = tokio::spawn(async move {
            let optionCal = c2.get_mut(&1);
            let mut val = optionCal.unwrap();
            val.set_age(12);

            println!("Done1")
        });

        future.await;
        future1.await;

        println!("{}", dashmap.get(&1).unwrap().age);
        return;
    }
}

