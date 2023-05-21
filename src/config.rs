use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryStoreConfig {
    pub capacity: i64
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LocalfileStoreConfig {
    pub data_paths: Vec<String>
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HybridStoreConfig {
    pub(crate) memory_should_spill_ratio: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Config {
    pub memory_store: Option<MemoryStoreConfig>,
    pub localfile_store: Option<LocalfileStoreConfig>,
    pub hybrid_store: Option<HybridStoreConfig>,

    pub store_type: Option<StorageType>
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageType {
    MEMORY,
    LOCALFILE,
    MEMORY_LOCALFILE
}

impl Config {
    fn get(conf_path: &str) -> Config {
        Config {
            memory_store: None,
            localfile_store: None,
            hybrid_store: None,
            store_type: None
        }
    }
}

#[cfg(test)]
mod test {
    use crate::config::Config;

    #[test]
    fn config_test() {
        let toml_str = r#"
        store_type = "MEMORY_LOCALFILE"

        [memory_store]
        capacity = 1024

        [localfile_store]
        data_paths = ["/data1/uniffle"]

        [hybrid_store]
        memory_should_spill_ratio = 0.7
        "#;

        let decoded: Config = toml::from_str(toml_str).unwrap();
        println!("{:#?}", decoded);
    }
}