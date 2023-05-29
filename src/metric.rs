use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use lazy_static::lazy_static;
use log::{error, info};

use prometheus::{HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry};
use warp::{Filter, Rejection, Reply};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    pub static ref TOTAL_RECEIVED_DATA: IntCounter = IntCounter::new("total_received_data", "Incoming Requests").expect("metric should be created");
    pub static ref TOTAL_MEMORY_USED: IntCounter = IntCounter::new("total_memory_used", "Total memory used").expect("metric should be created");
    pub static ref TOTAL_LOCALFILE_USED: IntCounter = IntCounter::new("total_localfile_used", "Total localfile used").expect("metric should be created");

    pub static ref GAUGE_MEMORY_USED: IntGauge = IntGauge::new("memory_used", "memory used").expect("metric should be created");
    pub static ref GAUGE_MEMORY_ALLOCATED: IntGauge = IntGauge::new("memory_allocated", "memory allocated").expect("metric should be created");
    pub static ref GAUGE_MEMORY_CAPACITY: IntGauge = IntGauge::new("memory_capacity", "memory capacity").expect("metric should be created");
}

fn register_custom_metrics() {
    REGISTRY.register(Box::new(TOTAL_RECEIVED_DATA.clone())).expect("total_received_data must be registered");
    REGISTRY.register(Box::new(TOTAL_MEMORY_USED.clone())).expect("total_memory_used must be registered");
    REGISTRY.register(Box::new(TOTAL_LOCALFILE_USED.clone())).expect("total_localfile_used must be registered");

    REGISTRY.register(Box::new(GAUGE_MEMORY_USED.clone())).expect("memory_used must be registered");
    REGISTRY.register(Box::new(GAUGE_MEMORY_ALLOCATED.clone())).expect("memory_allocated must be registered");
    REGISTRY.register(Box::new(GAUGE_MEMORY_CAPACITY.clone())).expect("memory_capacity must be registered");
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

pub fn start_metric_service(metric_service_http_port: i32) {
    register_custom_metrics();
    let metrics_route = warp::path!("metrics").and_then(metrics_handler);

    info!("Starting metric service with port:[{}] ......", metric_service_http_port);

    tokio::spawn(async move {
        warp::serve(metrics_route)
            .run(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), metric_service_http_port as u16))
            .await;
    });
}