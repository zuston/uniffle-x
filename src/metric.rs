use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use lazy_static::lazy_static;
use log::{error, info};

use prometheus::{HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry};
use warp::{Filter, Rejection, Reply};

lazy_static! {
     pub static ref REGISTRY: Registry = Registry::new();

     pub static ref INCOMING_REQUESTS: IntCounter = IntCounter::new("incoming_requests", "Incoming Requests").expect("metric should be created");
}

fn register_custom_metrics() {
    REGISTRY.register(Box::new(INCOMING_REQUESTS.clone())).expect("in_coming_requests has been registered")
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