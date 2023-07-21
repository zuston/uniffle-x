use poem::{get, RouteMethod};
use poem::endpoint::make;
use tokio::sync::Mutex;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::http::Handler;

pub struct AwaitTreeHandler {}

impl Default for AwaitTreeHandler {
    fn default() -> Self {
        Self {}
    }
}

impl Handler for AwaitTreeHandler {
    fn get_route_method(&self) -> RouteMethod {
        get(make(|_| async {
            let registry_cloned = AWAIT_TREE_REGISTRY.clone();
            let registry = registry_cloned.lock().await;
            let mut dynamic_string = String::new();
            for (_, tree) in registry.iter() {
                let raw_tree = format!("{}", tree);
                dynamic_string.push_str(raw_tree.as_str());
                dynamic_string.push('\n');
            }
            dynamic_string
        }))
    }

    fn get_route_path(&self) -> String {
        "/await-tree".to_string()
    }
}