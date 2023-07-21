use lazy_static::lazy_static;
use await_tree::Registry;
use tokio::sync::Mutex;
use std::sync::{Arc};
use std::time::Duration;
use log::info;

lazy_static! {
    pub static ref AWAIT_TREE_REGISTRY: Arc<Mutex<Registry<u64>>> = Arc::new(Mutex::new(Registry::new(await_tree::Config::default())));
}