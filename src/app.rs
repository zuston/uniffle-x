use std::borrow::BorrowMut;
use std::fs::read;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::AcqRel;
use std::time::Duration;
use dashmap::DashMap;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use dashmap::mapref::one::Ref;
use tokio::runtime::Runtime;
use tracing::callsite::register;
use crate::app::ReadOptions::Memory;
use crate::proto::uniffle::ShuffleData;
use crate::schedule::{ClientData, ClientDataBlock};

#[derive(Debug, Clone)]
enum DataDistribution {
    NORMAL,
    LOCAL_ORDER
}

#[derive(Debug, Clone)]
struct AppConfigOptions {
    data_distribution: DataDistribution,
    max_concurrency_per_partition_to_write: i32
}

// =============================================================

#[derive(Debug, Clone)]
pub struct App {
    appId: String,
    // key: shuffleId, value: partitionIds
    partitions: DashMap<i32, Vec<i32>>,
    is_alive: bool,
    configOptions: Option<AppConfigOptions>
}

unsafe impl Send for App {}

unsafe impl Sync for App {}

impl App {
    fn from(appId: String) -> Self {
        App {
            appId,
            partitions: DashMap::new(),
            is_alive: true,
            configOptions: None
        }
    }

    pub(crate) fn register_partition(&self, shuffleId: i32, partitionId: i32) {
        match self.partitions.get_mut(&partitionId) {
            Some(mut partitionIds) => partitionIds.push(partitionId),
            _ => {
                self.partitions.insert(shuffleId, vec!(partitionId));
            }
        }
    }

    fn mark_dead(&mut self) -> Result<()> {
        self.is_alive = false;
        Ok(())
    }

    pub fn put_data(&self, context: PartitionedDataContext, data: ClientData) -> Result<()> {
        Ok(())
    }

    pub fn get_data(&self, context: PartitionedDataContext, readOptions: ReadOptions, lastBlockId: i64) -> Result<()> {
        Ok(())
    }
}

// ==========================================================

#[derive(Debug)]
pub enum ReadOptions {
    Memory(MemoryReadOptions)
}

#[derive(Debug)]
pub struct MemoryReadOptions {
    buffer_size_per_read: i32
}

impl MemoryReadOptions {
    pub fn from(buffer_size: i32) -> MemoryReadOptions {
        MemoryReadOptions {
            buffer_size_per_read: buffer_size
        }
    }
}


// ==========================================================

pub type AppManagerRef = Arc<AppManager>;

pub struct AppManager {
    apps: Arc<DashMap<String, App>>
}

impl Default for AppManager {
    fn default() -> Self {
        let manager = AppManager {
            apps: Arc::new(DashMap::new())
        };
        manager
    }
}

impl AppManager {
    pub(crate) fn get_ref() -> AppManagerRef {
        let appRef = Arc::new(AppManager::default());

        let appManagerRefClone = appRef.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;

                for item in appManagerRefClone.apps.iter() {
                    let (key, val) = item.pair();
                }
            }
        });
        appRef
    }

    pub(crate) fn keep_app_alive(&self, appId: String) {
        todo!()
    }

    pub(crate) fn unregister(&self, appId: String, shuffleId: Option<i32>) {
        todo!()
    }

    // todo: 支持从客户端传入参数
    pub(crate) fn register(&self, appId: String) -> Result<()> {
        match self.apps.get(&appId) {
            Some(app) => (),
            _ => {
                let app = App::from(appId.clone());
                self.apps.insert(appId.clone(), app);
            }
        }
        Ok(())
    }

    pub(crate) fn get_app(&self, appId: String) -> Option<App> {
        self.apps.get(&appId).map(|v| v.value().clone())
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Default, Debug, Hash, Clone)]
pub struct PartitionedDataContext {
    appId: String,
    shuffleId: i32,
    partitionId: i32
}

impl PartitionedDataContext {

    pub(crate) fn from(appId: String, shuffleId: i32, partitionId: i32) -> PartitionedDataContext {
        PartitionedDataContext {
            appId,
            shuffleId,
            partitionId
        }
    }
}

#[cfg(test)]
mod test {
    use crate::app::{AppManager, PartitionedDataContext};

    #[tokio::test]
    async fn app_manager_test() {
        let appManagerRef = AppManager::get_ref().clone();
        if let Ok(_) = appManagerRef.register("app_id".into()) {
            if let Some(app) = appManagerRef.get_app("app_id".into()) {
                assert_eq!("app_id", app.appId);
            }
        }
    }
}

