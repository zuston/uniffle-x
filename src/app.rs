use std::borrow::BorrowMut;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::fs::read;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::ops::Deref;
use std::sync::{Arc, mpsc, Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::atomic::Ordering::AcqRel;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use anyhow::{Result, anyhow};
use bytes::{BufMut, Bytes, BytesMut};
use croaring::Treemap;
use croaring::treemap::JvmSerializer;
use dashmap::mapref::one::{Ref, RefMut};
use log::{debug, error, info};
use tokio::runtime::Runtime;
use tonic::codegen::ok;
use crate::config::Config;
use crate::error::DatanodeError;
use crate::metric::{GAUGE_APP_NUMBER, GAUGE_PARTITION_NUMBER, TOTAL_APP_NUMBER, TOTAL_RECEIVED_DATA};
use crate::proto::uniffle::ShuffleData;
use crate::store;
use crate::store::{PartitionedData, PartitionedDataBlock, ResponseData, ResponseDataIndex, Store, StoreProvider};
use crate::store::hybrid::HybridStore;
use crate::store::memory::{MemorySnapshot, MemoryStore};
use crate::util::current_timestamp_sec;

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
    bitmap_of_blocks: DashMap<i32, DashMap<i32, Arc<RwLock<Treemap>>>>
}

impl App {
    fn from(app_id: String, config_options: Option<AppConfigOptions>, store: Arc<HybridStore>) -> Self {
        App {
            app: Arc::new(
                AppInner {
                    app_id,
                    partitions: DashMap::new(),
                    app_config_options: config_options,
                    latest_heartbeat_time: AtomicU64::new(current_timestamp_sec())
                },
            ),
            store,
            bitmap_of_blocks: DashMap::new()
        }
    }

    fn get_latest_heartbeat_time(&self) -> u64 {
        self.app.latest_heartbeat_time.load(Ordering::SeqCst)
    }

    pub fn heartbeat(&self) -> Result<()> {
        let timestamp = current_timestamp_sec();
        self.app.latest_heartbeat_time.swap(timestamp, Ordering::SeqCst);
        Ok(())
    }

    pub fn register_shuffle(&self, shuffle_id: i32) -> Result<()> {
        self.app.partitions.entry(shuffle_id).or_insert_with(||HashSet::new());
        Ok(())
    }

    pub async fn insert(&self, ctx: WritingViewContext) -> Result<()> {
        let len: i32 = ctx.data_blocks.iter().map(|block| block.length).sum();
        TOTAL_RECEIVED_DATA.inc_by(len as u64);
        
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

    fn get_underlying_partition_bitmap(&self, uid: PartitionedUId) -> Arc<RwLock<Treemap>> {
        let shuffle_id = uid.shuffle_id;
        let partition_id = uid.partition_id;
        let shuffle_entry = self.bitmap_of_blocks.entry(shuffle_id).or_insert_with(||DashMap::new());
        let mut partition_bitmap = shuffle_entry.entry(partition_id).or_insert_with(||Arc::new(RwLock::new(Treemap::create())));

        partition_bitmap.clone()
    }

    pub async fn get_block_ids(&self, ctx: GetBlocksContext) -> Result<Bytes> {
        debug!("get blocks: {:?}", ctx.clone());
        let mut partition_bitmap = self.get_underlying_partition_bitmap(ctx.uid);
        let partition_bitmap = partition_bitmap.read().unwrap();

        let serialized_data = partition_bitmap.serialize()?;
        Ok(
            Bytes::from(serialized_data)
        )
    }

    pub async fn report_block_ids(&self, ctx: ReportBlocksContext) -> Result<()> {
        debug!("Report blocks: {:?}", ctx.clone());
        let mut partition_bitmap_wrapper = self.get_underlying_partition_bitmap(ctx.uid);
        let mut partition_bitmap = partition_bitmap_wrapper.write().unwrap();

        for block_id in ctx.blocks {
            partition_bitmap.add(block_id as u64);
        }

        Ok(())
    }

    pub async fn purge(&self, app_id: String, shuffle_id: Option<i32>) -> Result<()> {
        if shuffle_id.is_some() {
            error!("Partial purge is not supported.");
        } else {
            self.store.purge(app_id).await?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReportBlocksContext {
    pub(crate) uid: PartitionedUId,
    pub(crate) blocks: Vec<i64>
}

#[derive(Debug, Clone)]
pub struct GetBlocksContext {
    pub(crate) uid: PartitionedUId,
}

#[derive(Debug)]
pub struct AppInner {
    app_id: String,
    // key: shuffleId, value: partitionIds
    partitions: DashMap<i32, HashSet<i32>>,
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
    // key: app_id
    apps: DashMap<String, Arc<App>>,
    receiver: async_channel::Receiver<PurgeEvent>,
    sender: async_channel::Sender<PurgeEvent>,
    store: Arc<HybridStore>,
    app_heartbeat_timeout_min: u32
}

impl AppManager {
    fn new(config: Config) -> Self {
        let (sender, receiver) = async_channel::unbounded();
        let app_heartbeat_timeout_min = config.app_heartbeat_timeout_min.unwrap_or(10);
        let store = Arc::new(StoreProvider::get(config));
        store.clone().start();
        let manager = AppManager {
            apps: DashMap::new(),
            receiver,
            sender,
            store,
            app_heartbeat_timeout_min
        };
        manager
    }
}

impl AppManager {
    pub fn get_ref(config: Config) -> AppManagerRef {
        let app_ref = Arc::new(AppManager::new(config));
        let app_manager_ref_cloned = app_ref.clone();

        tokio::spawn(async move {
            info!("Starting app heartbeat checker...");
            loop {
                // task1: find out heartbeat timeout apps
                tokio::time::sleep(Duration::from_secs(1)).await;

                let current_timestamp = current_timestamp_sec();
                for item in app_manager_ref_cloned.apps.iter() {
                    let (key, app) = item.pair();
                    let last_time = app.get_latest_heartbeat_time();

                    if current_timestamp_sec() - last_time > (app_manager_ref_cloned.app_heartbeat_timeout_min * 60) as u64 {
                        if app_manager_ref_cloned.sender.send(PurgeEvent::HEART_BEAT_TIMEOUT(key.clone())).await.is_err() {
                            error!("Errors on sending purge event when app: {} heartbeat timeout", key);
                        }
                    }
                }
            }
        });

        let app_manager_cloned = app_ref.clone();
        tokio::spawn(async move {
            info!("Starting purge event handler...");
            while let Ok(event) = app_manager_cloned.receiver.recv().await {
                GAUGE_APP_NUMBER.dec();
                let _ = match event {
                    PurgeEvent::HEART_BEAT_TIMEOUT(app_id) => {
                        info!("The app:{} data of heartbeat timeout will be purged.", &app_id);
                        app_manager_cloned.purge_app_data(app_id).await
                    },
                    PurgeEvent::APP_PURGE(app_id) => {
                        info!("The app:{} has been finished, its data will be purged.", &app_id);
                        app_manager_cloned.purge_app_data(app_id).await
                    },
                    PurgeEvent::APP_PARTIAL_SHUFFLES_PURGE(app_id, shuffle_ids) => {
                        info!("Partial data purge is not supported currently");
                        Ok(())
                    }
                }.map_err(|err| error!("Errors on purging data. error: {:?}", err));
            }
        });

        app_ref
    }

    pub async fn store_is_healthy(&self) -> Result<bool> {
        self.store.is_healthy().await
    }

    pub async fn store_memory_snapshot(&self) -> Result<MemorySnapshot> {
        self.store.get_hot_store_memory_snapshot().await
    }

    pub fn store_memory_spill_event_num(&self) -> Result<u64> {
        self.store.memory_spill_event_num()
    }

    async fn purge_app_data(&self, app_id: String) -> Result<()> {
        let app = self.get_app(&app_id);
        if app.is_none() {
            error!("App:{} don't exist when purging data, this should not happen", &app_id);
        } else {
            let app = app.unwrap();
            app.purge(app_id.clone(), None).await?;
        }

        self.apps.remove(&app_id);

        Ok(())
    }

    pub fn get_app(&self, app_id: &str) -> Option<Arc<App>> {
        self.apps.get(app_id).map(|v| v.value().clone())
    }

    pub fn register(&self, app_id: String, shuffle_id: i32) -> Result<()> {
        info!("Accepted registry. app_id: {}, shuffle_id: {}", app_id.clone(), shuffle_id);
        let mut appRef = self.apps.entry(app_id.clone()).or_insert_with(|| {
            TOTAL_APP_NUMBER.inc();
            GAUGE_APP_NUMBER.inc();
            Arc::new(App::from(app_id, None, self.store.clone()))
        });
        appRef.register_shuffle(shuffle_id)
    }

    pub async fn unregister(&self, app_id: String, shuffle_ids: Option<Vec<i32>>) -> Result<()> {
        let event = match shuffle_ids {
            Some(ids) => PurgeEvent::APP_PARTIAL_SHUFFLES_PURGE(app_id, ids),
            _ => PurgeEvent::APP_PURGE(app_id)
        };

        self.sender.send(event).await?;
        Ok(())
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Default, Debug, Hash, Clone)]
pub struct PartitionedUId {
    pub app_id: String,
    pub shuffle_id: i32,
    pub partition_id: i32
}

impl PartitionedUId {
    pub fn from(app_id: String, shuffle_id: i32, partition_id: i32) -> PartitionedUId {
        PartitionedUId {
            app_id,
            shuffle_id,
            partition_id
        }
    }

    pub fn get_hash(uid: &PartitionedUId) -> u64 {
        let mut hasher = DefaultHasher::new();

        uid.hash(&mut hasher);
        let hash_value = hasher.finish();

        hash_value
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use croaring::Treemap;
    use croaring::treemap::JvmSerializer;
    use dashmap::DashMap;
    use crate::app::{AppManager, GetBlocksContext, PartitionedUId, ReadingOptions, ReadingViewContext, ReportBlocksContext, WritingViewContext};
    use crate::Config;
    use crate::config::{HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig};
    use crate::store::{PartitionedDataBlock, ResponseData};
    use crate::store::hybrid::HybridStore;

    #[test]
    fn test_uid_hash() {
        let uid = PartitionedUId::from("a".to_string(), 1, 1);
        let hash_value = PartitionedUId::get_hash(&uid);
        println!("{}", hash_value);
    }

    fn mock_config() -> Config {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let mut config = Config::default();
        config.memory_store=Some(MemoryStoreConfig {
            capacity: (1024 * 1024).to_string()
        });
        config.localfile_store = Some(LocalfileStoreConfig::new(vec![temp_path]));
        config.hybrid_store = Some(HybridStoreConfig::default());
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
    async fn test_get_or_put_block_ids() {
        let app_id = "test_get_or_put_block_ids-----id".to_string();

        let appManagerRef = AppManager::get_ref(mock_config()).clone();
        appManagerRef.register(app_id.clone().into(), 1).unwrap();

        let app = appManagerRef.get_app(app_id.as_ref()).unwrap();
        app.report_block_ids(ReportBlocksContext {
            uid: PartitionedUId {
                app_id: app_id.clone(),
                shuffle_id: 1,
                partition_id: 0
            },
            blocks: vec![
                123,
                124
            ]
        }).await.expect("TODO: panic message");

        let data = app.get_block_ids(GetBlocksContext {
            uid: PartitionedUId {
                app_id,
                shuffle_id: 1,
                partition_id: 0
            }
        }).await.expect("TODO: panic message");

        let deserialized = Treemap::deserialize(&data).unwrap();
        assert_eq!(deserialized, Treemap::from_iter(vec![123, 124]));
    }
}

