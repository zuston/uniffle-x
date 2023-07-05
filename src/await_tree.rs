use lazy_static::lazy_static;
use await_tree::Registry;
use tokio::sync::Mutex;
use std::sync::{Arc};
use std::time::Duration;
use log::info;

lazy_static! {
    pub static ref AWAIT_TREE_REGISTRY: Arc<Mutex<Registry<u64>>> = {
        let registry = Arc::new(Mutex::new(Registry::new(await_tree::Config::default())));
        schedule_print_out_await_tree(registry.clone());
        registry
    };
}

fn schedule_print_out_await_tree(registry: Arc<Mutex<Registry<u64>>>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let temp_registry = registry.lock().await;
            info!("========================== await tree ==========================");
            for (_, tree) in temp_registry.iter() {
                info!("{tree}");
            }
        }
    });
}