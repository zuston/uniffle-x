use std::borrow::BorrowMut;
use std::fs::read;
use std::io::Read;
use std::ops::Deref;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::atomic::Ordering::AcqRel;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use anyhow::{Result, anyhow};
use bytes::{BufMut, Bytes};
use dashmap::mapref::one::Ref;
use tokio::runtime::Runtime;
use tonic::codegen::ok;
use tracing::callsite::register;
use crate::error::DatanodeError;
use crate::proto::uniffle::ShuffleData;
use crate::store::{MemoryStore, PartitionedData, PartitionedDataBlock, ResponseData, ResponseDataIndex, Store};

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
    store: Arc<MemoryStore>
}

impl App {
    fn from(app_id: String, config_options: Option<AppConfigOptions>, store: Arc<MemoryStore>) -> Self {
        App {
            app: Arc::new(
                AppInner {
                    app_id,
                    partitions: DashMap::new(),
                    app_config_options: config_options,
                    latest_heartbeat_time: AtomicU64::new(App::current_timestamp())
                },
            ),
            store
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

    pub fn insert(&mut self, ctx: WritingViewContext) -> Result<()> {
        let mut store_mut = Arc::make_mut(&mut self.store);
        store_mut.insert(ctx)
    }

    pub fn select(&mut self, ctx: ReadingViewContext) -> Result<ResponseData> {
        // let mut store_mut = Arc::make_mut(&mut self.store);
        // store_mut.get(ctx)
        self.store.get(ctx)
    }

    pub fn list_index(&mut self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex> {
        let mut store_mut = Arc::make_mut(&mut self.store);
        store_mut.get_index(ctx)
    }

    pub fn purge(&self, app_id: String, shuffle_id: Option<i32>) -> Result<()> {
        Ok(())
    }
}


#[derive(Debug)]
pub struct AppInner {
    app_id: String,
    // key: shuffleId, value: partitionIds
    partitions: DashMap<i32, Vec<i32>>,
    app_config_options: Option<AppConfigOptions>,
    latest_heartbeat_time: AtomicU64
}

pub struct WritingViewContext {
    pub uid: PartitionedUId,
    pub data_blocks: Vec<PartitionedDataBlock>
}

pub struct ReadingViewContext {
    pub uid: PartitionedUId,
    pub reading_options: ReadingOptions
}

pub struct ReadingIndexViewContext {
    partition_id: PartitionedUId
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
    store: Arc<MemoryStore>
}

impl Default for AppManager {
    fn default() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let manager = AppManager {
            apps: DashMap::new(),
            receiver,
            sender,
            store: Arc::new(MemoryStore::new(
                1024 * 1024 * 1024 * 100
            ))
        };
        manager
    }
}

impl AppManager {
    pub(crate) fn get_ref() -> AppManagerRef {
        let app_ref = Arc::new(AppManager::default());

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

    pub(crate) fn get_app(&self, app_id: String) -> Option<App> {
        self.apps.get(&app_id).map(|v| v.value().clone())
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
    use crate::app::{AppManager, PartitionedUId};

    #[tokio::test]
    async fn app_manager_test() {
        let appManagerRef = AppManager::get_ref().clone();
        appManagerRef.register("app_id".into(), 1).unwrap();
        if let Some(app) = appManagerRef.get_app("app_id".into()) {
            assert_eq!("app_id", app.app.app_id);
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

