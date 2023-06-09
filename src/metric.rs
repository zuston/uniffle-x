use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use lazy_static::lazy_static;
use log::{error, info};

use prometheus::{HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, labels, Opts, Registry};
use tracing_subscriber::fmt::time;
use warp::{Filter, Rejection, Reply};
use crate::config::MetricsConfig;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    pub static ref TOTAL_RECEIVED_DATA: IntCounter = IntCounter::new("total_received_data", "Incoming Requests").expect("metric should be created");
    pub static ref TOTAL_MEMORY_USED: IntCounter = IntCounter::new("total_memory_used", "Total memory used").expect("metric should be created");
    pub static ref TOTAL_LOCALFILE_USED: IntCounter = IntCounter::new("total_localfile_used", "Total localfile used").expect("metric should be created");

    pub static ref GAUGE_MEMORY_USED: IntGauge = IntGauge::new("memory_used", "memory used").expect("metric should be created");
    pub static ref GAUGE_MEMORY_ALLOCATED: IntGauge = IntGauge::new("memory_allocated", "memory allocated").expect("metric should be created");
    pub static ref GAUGE_MEMORY_CAPACITY: IntGauge = IntGauge::new("memory_capacity", "memory capacity").expect("metric should be created");

    pub static ref TOTAL_MEMORY_SPILL_OPERATION: IntCounter = IntCounter::new("total_memory_spill_operation", "memory capacity").expect("metric should be created");
    pub static ref TOTAL_MEMORY_SPILL_OPERATION_FAILED: IntCounter = IntCounter::new("total_memory_spill_operation_failed", "memory capacity").expect("metric should be created");

    pub static ref GAUGE_MEMORY_SPILL_OPERATION: IntGauge = IntGauge::new("memory_spill_operation", "memory spill").expect("metric should be created");

    pub static ref TOTAL_APP_NUMBER : IntCounter = IntCounter::new("total_app_number", "total_app_number").expect("metrics should be created");
    pub static ref TOTAL_PARTITION_NUMBER: IntCounter = IntCounter::new("total_partition_number", "total_partition_number").expect("metrics should be created");

    pub static ref GAUGE_APP_NUMBER: IntGauge = IntGauge::new("app_number", "app_number").expect("metrics should be created");
    pub static ref GAUGE_PARTITION_NUMBER: IntGauge = IntGauge::new("partition_number", "partition_number").expect("metrics should be created");
}

fn register_custom_metrics() {
    REGISTRY.register(Box::new(TOTAL_RECEIVED_DATA.clone())).expect("total_received_data must be registered");
    REGISTRY.register(Box::new(TOTAL_MEMORY_USED.clone())).expect("total_memory_used must be registered");
    REGISTRY.register(Box::new(TOTAL_LOCALFILE_USED.clone())).expect("total_localfile_used must be registered");
    REGISTRY.register(Box::new(TOTAL_MEMORY_SPILL_OPERATION.clone())).expect("total_memory_spill_operation must be registered");
    REGISTRY.register(Box::new(TOTAL_MEMORY_SPILL_OPERATION_FAILED.clone())).expect("total_memory_spill_operation_failed must be registered");
    REGISTRY.register(Box::new(TOTAL_APP_NUMBER.clone())).expect("total_app_number must be registered");
    REGISTRY.register(Box::new(TOTAL_PARTITION_NUMBER.clone())).expect("total_partition_number must be registered");

    REGISTRY.register(Box::new(GAUGE_MEMORY_USED.clone())).expect("memory_used must be registered");
    REGISTRY.register(Box::new(GAUGE_MEMORY_ALLOCATED.clone())).expect("memory_allocated must be registered");
    REGISTRY.register(Box::new(GAUGE_MEMORY_CAPACITY.clone())).expect("memory_capacity must be registered");
    REGISTRY.register(Box::new(GAUGE_APP_NUMBER.clone())).expect("app_number must be registered");
    REGISTRY.register(Box::new(GAUGE_PARTITION_NUMBER.clone())).expect("partition_number must be registered");
    REGISTRY.register(Box::new(GAUGE_MEMORY_SPILL_OPERATION.clone())).expect("memory_spill_operation must be registered");
}

async fn metrics_handler() -> Result<impl Reply, Rejection> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        error!("could not encode custom metrics: {:?}", e);
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("custom metrics could not be from_utf8'd: {:?}", e);
            String::default()
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        error!("could not encode prometheus metrics: {:?}", e);
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("prometheus metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    res.push_str(&res_custom);
    Ok(res)
}

pub fn start_metric_service(metric_config: &Option<MetricsConfig>, datanode_uid: String) {
    if metric_config.is_none() {
        return;
    }

    register_custom_metrics();

    let default_http_port = 19998;
    let job_name = "uniffle-datanode";

    let cfg = metric_config.clone().unwrap();

    let metrics_route = warp::path!("metrics").and_then(metrics_handler);

    let http_port = cfg.http_port.unwrap_or(default_http_port).clone();
    info!("Starting metric service with port:[{}] ......", http_port);

    let push_gateway_endpoint = cfg.push_gateway_endpoint;

    if let Some(ref endpoint) = push_gateway_endpoint {
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
                    labels! {"datanode_id".to_owned() => datanode_uid.to_owned(),},
                    &push_gateway_endpoint.to_owned().unwrap().to_owned(),
                    metrics,
                    None
                );
                if pushed_result.is_err() {
                    error!("Errors on pushing metrics. {:?}", pushed_result.err());
                }
            }
        });
    }

    tokio::spawn(async move {
        warp::serve(metrics_route)
            .run(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), http_port as u16))
            .await;
    });
}