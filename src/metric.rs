use crate::config::MetricsConfig;
use lazy_static::lazy_static;
use log::{error, info};
use prometheus::{labels, IntCounter, IntGauge, Registry};
use std::time::Duration;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref TOTAL_RECEIVED_DATA: IntCounter =
        IntCounter::new("total_received_data", "Incoming Requests")
            .expect("metric should be created");
    pub static ref TOTAL_MEMORY_USED: IntCounter =
        IntCounter::new("total_memory_used", "Total memory used")
            .expect("metric should be created");
    pub static ref TOTAL_LOCALFILE_USED: IntCounter =
        IntCounter::new("total_localfile_used", "Total localfile used")
            .expect("metric should be created");
    pub static ref TOTAL_HDFS_USED: IntCounter =
        IntCounter::new("total_hdfs_used", "Total hdfs used").expect("metric should be created");
    pub static ref GAUGE_MEMORY_USED: IntGauge =
        IntGauge::new("memory_used", "memory used").expect("metric should be created");
    pub static ref GAUGE_MEMORY_ALLOCATED: IntGauge =
        IntGauge::new("memory_allocated", "memory allocated").expect("metric should be created");
    pub static ref GAUGE_MEMORY_CAPACITY: IntGauge =
        IntGauge::new("memory_capacity", "memory capacity").expect("metric should be created");
    pub static ref TOTAL_MEMORY_SPILL_OPERATION: IntCounter =
        IntCounter::new("total_memory_spill", "memory capacity").expect("metric should be created");
    pub static ref TOTAL_MEMORY_SPILL_OPERATION_FAILED: IntCounter =
        IntCounter::new("total_memory_spill_failed", "memory capacity")
            .expect("metric should be created");
    pub static ref TOTAL_MEMORY_SPILL_TO_LOCALFILE: IntCounter = IntCounter::new(
        "total_memory_spill_to_localfile",
        "memory spill to localfile"
    )
    .expect("metric should be created");
    pub static ref TOTAL_MEMORY_SPILL_TO_HDFS: IntCounter =
        IntCounter::new("total_memory_spill_to_hdfs", "memory spill to hdfs")
            .expect("metric should be created");
    pub static ref GAUGE_MEMORY_SPILL_OPERATION: IntGauge =
        IntGauge::new("memory_spill", "memory spill").expect("metric should be created");
    pub static ref GAUGE_MEMORY_SPILL_TO_LOCALFILE: IntGauge =
        IntGauge::new("memory_spill_to_localfile", "memory spill to localfile")
            .expect("metric should be created");
    pub static ref GAUGE_MEMORY_SPILL_TO_HDFS: IntGauge =
        IntGauge::new("memory_spill_to_hdfs", "memory spill to hdfs")
            .expect("metric should be created");
    pub static ref TOTAL_APP_NUMBER: IntCounter =
        IntCounter::new("total_app_number", "total_app_number").expect("metrics should be created");
    pub static ref TOTAL_PARTITION_NUMBER: IntCounter =
        IntCounter::new("total_partition_number", "total_partition_number")
            .expect("metrics should be created");
    pub static ref GAUGE_APP_NUMBER: IntGauge =
        IntGauge::new("app_number", "app_number").expect("metrics should be created");
    pub static ref GAUGE_PARTITION_NUMBER: IntGauge =
        IntGauge::new("partition_number", "partition_number").expect("metrics should be created");
    pub static ref TOTAL_REQUIRE_BUFFER_FAILED: IntCounter =
        IntCounter::new("total_require_buffer_failed", "total_require_buffer_failed")
            .expect("metrics should be created");
    pub static ref TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED: IntCounter = IntCounter::new(
        "total_huge_partition_require_buffer_failed",
        "total_huge_partition_require_buffer_failed"
    )
    .expect("metrics should be created");
}

fn register_custom_metrics() {
    REGISTRY
        .register(Box::new(TOTAL_RECEIVED_DATA.clone()))
        .expect("total_received_data must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_USED.clone()))
        .expect("total_memory_used must be registered");
    REGISTRY
        .register(Box::new(TOTAL_LOCALFILE_USED.clone()))
        .expect("total_localfile_used must be registered");
    REGISTRY
        .register(Box::new(TOTAL_HDFS_USED.clone()))
        .expect("total_hdfs_used must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_OPERATION.clone()))
        .expect("total_memory_spill_operation must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_OPERATION_FAILED.clone()))
        .expect("total_memory_spill_operation_failed must be registered");
    REGISTRY
        .register(Box::new(TOTAL_APP_NUMBER.clone()))
        .expect("total_app_number must be registered");
    REGISTRY
        .register(Box::new(TOTAL_PARTITION_NUMBER.clone()))
        .expect("total_partition_number must be registered");
    REGISTRY
        .register(Box::new(TOTAL_REQUIRE_BUFFER_FAILED.clone()))
        .expect("total_require_buffer_failed must be registered");
    REGISTRY
        .register(Box::new(TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED.clone()))
        .expect("total_huge_partition_require_buffer_failed must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_TO_LOCALFILE.clone()))
        .expect("total_memory_spill_to_localfile must be registered");
    REGISTRY
        .register(Box::new(TOTAL_MEMORY_SPILL_TO_HDFS.clone()))
        .expect("total_memory_spill_to_hdfs must be registered");

    REGISTRY
        .register(Box::new(GAUGE_MEMORY_USED.clone()))
        .expect("memory_used must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_ALLOCATED.clone()))
        .expect("memory_allocated must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_CAPACITY.clone()))
        .expect("memory_capacity must be registered");
    REGISTRY
        .register(Box::new(GAUGE_APP_NUMBER.clone()))
        .expect("app_number must be registered");
    REGISTRY
        .register(Box::new(GAUGE_PARTITION_NUMBER.clone()))
        .expect("partition_number must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_OPERATION.clone()))
        .expect("memory_spill_operation must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_TO_LOCALFILE.clone()))
        .expect("memory_spill_to_localfile must be registered");
    REGISTRY
        .register(Box::new(GAUGE_MEMORY_SPILL_TO_HDFS.clone()))
        .expect("memory_spill_to_hdfs must be registered");
}

pub fn configure_metric_service(metric_config: &Option<MetricsConfig>, worker_uid: String) -> bool {
    if metric_config.is_none() {
        return false;
    }

    register_custom_metrics();

    let job_name = "uniffle-worker";

    let cfg = metric_config.clone().unwrap();

    let push_gateway_endpoint = cfg.push_gateway_endpoint;

    if let Some(ref _endpoint) = push_gateway_endpoint {
        let push_interval_sec = cfg.push_interval_sec.unwrap_or(60);
        tokio::spawn(async move {
            info!("Starting prometheus metrics exporter...");
            loop {
                tokio::time::sleep(Duration::from_secs(push_interval_sec as u64)).await;

                let general_metrics = prometheus::gather();
                let custom_metrics = REGISTRY.gather();
                let mut metrics = vec![];
                metrics.extend_from_slice(&custom_metrics);
                metrics.extend_from_slice(&general_metrics);

                let pushed_result = prometheus::push_metrics(
                    job_name,
                    labels! {"worker_id".to_owned() => worker_uid.to_owned(),},
                    &push_gateway_endpoint.to_owned().unwrap().to_owned(),
                    metrics,
                    None,
                );
                if pushed_result.is_err() {
                    error!("Errors on pushing metrics. {:?}", pushed_result.err());
                }
            }
        });
    }
    return true;
}
