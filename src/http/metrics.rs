use log::error;
use poem::{get, RouteMethod};
use poem::endpoint::make_sync;
use crate::http::Handler;
use crate::metric::REGISTRY;

pub struct MetricsHTTPHandler {}

impl Default for MetricsHTTPHandler {
    fn default() -> Self {
        Self {}
    }
}

impl Handler for MetricsHTTPHandler {
    fn get_route_method(&self) -> RouteMethod {
        get(make_sync(|_| {
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
            res
        }))
    }

    fn get_route_path(&self) -> String {
        "/metrics".to_string()
    }
}