use std::borrow::BorrowMut;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::AcqRel;
use dashmap::DashMap;
use anyhow::{Result, anyhow};
use dashmap::mapref::one::Ref;
use tracing::callsite::register;

#[derive(Debug, Clone)]
struct App {
    appId: String,
    // key: shuffleId, value: partitionIds
    partitions: DashMap<i32, Vec<i32>>,
    is_alive: bool,
}

unsafe impl Send for App {}

unsafe impl Sync for App {}

impl App {
    fn from(appId: String) -> Self {
        App {
            appId,
            partitions: DashMap::new(),
            is_alive: true
        }
    }

    fn registerPartition(&self, shuffleId: i32, partitionId: i32) {
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
}

// ==========================================================

pub type AppManagerRef = Arc<AppManager>;

pub struct AppManager {
    apps: DashMap<String, App>
}

impl Default for AppManager {
    fn default() -> Self {
        AppManager {
            apps: DashMap::new()
        }
    }
}


impl AppManager {
    fn get_ref() -> AppManagerRef {
        Arc::new(AppManager::default())
    }

    fn register(&self, context: PartitionedDataContext) -> Result<()> {
        match self.apps.get(&context.appId) {
            Some(app) => app.registerPartition(context.shuffleId, context.partitionId),
            _ => {
                let app = App::from(context.appId.clone());
                app.registerPartition(context.shuffleId, context.partitionId);
                self.apps.insert(context.appId.clone(), app);
            }
        }
        Ok(())
    }

    fn get_app(&self, appId: String) -> Option<App> {
        self.apps.get(&appId).map(|v| v.value().clone())
    }
}

struct PartitionedDataContext {
    appId: String,
    shuffleId: i32,
    partitionId: i32
}

impl PartitionedDataContext {

    fn from(appId: String, shuffleId: i32, partitionId: i32) -> PartitionedDataContext {
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

    #[test]
    fn app_manager_test() {
        let appManagerRef = AppManager::get_ref();
        let context = PartitionedDataContext::from(
            "app_id".into(),
            1,
            1
        );
        if let Ok(_) = appManagerRef.register(context) {
            if let Some(app) = appManagerRef.get_app("app_id".into()) {
                assert_eq!("app_id", app.appId);
                assert_eq!(1, app.partitions.len());
            }
        }
    }
}

