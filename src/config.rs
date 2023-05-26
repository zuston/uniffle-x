use std::fs;
use std::path::Path;
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
    pub memory_spill_high_watermark: f32,
    pub memory_spill_low_watermark: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct Config {
    pub memory_store: Option<MemoryStoreConfig>,
    pub localfile_store: Option<LocalfileStoreConfig>,
    pub hybrid_store: Option<HybridStoreConfig>,

    pub store_type: Option<StorageType>,

    pub grpc_port: Option<i32>,
    pub coordinator_quorum: Vec<String>
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageType {
    MEMORY,
    LOCALFILE,
    MEMORY_LOCALFILE
}

const CONFIG_FILE_PATH_KEY: &str = "DATANODE_CONFIG_PATH";

impl Config {
    pub fn create_from_env() -> Config {
        let path = match std::env::var(CONFIG_FILE_PATH_KEY) {
            Ok(val) => val,
            _ => panic!("config path must be set in env args. key: {}", CONFIG_FILE_PATH_KEY)
        };

        let path = Path::new(&path);

        // Read the file content as a string
        let file_content = fs::read_to_string(path).expect("Failed to read file");

        toml::from_str(&file_content).unwrap()
    }

    pub fn create(conf_path: &str) -> Config {
        Config {
            memory_store: None,
            localfile_store: None,
            hybrid_store: None,
            store_type: None,
            grpc_port: None,
            coordinator_quorum: vec!["xxxxxx".to_string()]
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
        "#;

        let decoded: Config = toml::from_str(toml_str).unwrap();
        println!("{:#?}", decoded);
    }
}