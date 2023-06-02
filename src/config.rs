use std::fs;
use std::path::Path;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryStoreConfig {
    pub capacity: String
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LocalfileStoreConfig {
    pub data_paths: Vec<String>,
    pub disk_high_watermark: Option<f32>,
    pub disk_low_watermark: Option<f32>
}

impl LocalfileStoreConfig {
    pub fn new(data_paths: Vec<String>) -> Self {
        LocalfileStoreConfig {
            data_paths,
            disk_high_watermark: None,
            disk_low_watermark: None
        }
    }
}

// =========================================================

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

    pub metrics: Option<MetricsConfig>,

    pub grpc_port: Option<i32>,
    pub coordinator_quorum: Vec<String>,
    pub tags: Option<Vec<String>>,

    pub log: Option<LogConfig>,

    pub app_heartbeat_timeout_min: Option<u32>
}

// =========================================================
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MetricsConfig {
    pub http_port: Option<u32>,
    pub push_gateway_endpoint: Option<String>,
    pub push_interval_sec: Option<u32>
}

// =========================================================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LogConfig {
    pub path: String,
    pub rotation: RotationConfig,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            path: "/tmp/".to_string(),
            rotation: RotationConfig::Hourly
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RotationConfig {
    Hourly,
    Daily,
    Never,
}

// =========================================================

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
}

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use crate::config::Config;
    use crate::readable_size::ReadableSize;

    #[test]
    fn config_test() {
        let toml_str = r#"
        store_type = "MEMORY_LOCALFILE"
        coordinator_quorum = ["xxxxxxx"]

        [memory_store]
        capacity = "1024M"

        [localfile_store]
        data_paths = ["/data1/uniffle"]
        "#;

        let decoded: Config = toml::from_str(toml_str).unwrap();
        println!("{:#?}", decoded);

        let capacity = ReadableSize::from_str(&decoded.memory_store.unwrap().capacity).unwrap();
        println!("{}", capacity.as_bytes());
    }
}