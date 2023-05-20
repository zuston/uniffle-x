use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryStoreConfig {
    capacity: i64
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LocalfileStoreConfig {
    data_paths: Vec<String>
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HybridStoreConfig {
    // todo: support memory/low watermark
    // memory_should_spill_ratio: f32,
}

#[derive(Deserialize)]
#[derive(Debug)]
pub struct Config {
    memory_store: Option<MemoryStoreConfig>,
    local_store: Option<LocalfileStoreConfig>
}

#[cfg(test)]
mod test {
    fn config_test() {
        let file_content = r#"
        [Storage]
        stoargeType=memory
        "#;
    }
}