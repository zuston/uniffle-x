mod pprof;
mod http_service;
mod metrics;

use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;
use poem::RouteMethod;
use crate::http::http_service::PoemHTTPServer;
use crate::http::metrics::MetricsHTTPHandler;
use crate::http::pprof::PProfHandler;
use crate::metric::register_custom_metrics;

lazy_static! {
    pub static ref HTTP_SERVICE: Box<PoemHTTPServer> = new_server();
}

/// Implement the own handlers for concrete components
pub trait Handler {
    fn get_route_method(&self) -> RouteMethod;
    fn get_route_path(&self) -> String;
}

pub trait HTTPServer: Send + Sync {
    fn start(&self, port: u16);
    fn register_handler(&self, handler: impl Handler + 'static);
}

fn new_server() -> Box<PoemHTTPServer> {
    let server = PoemHTTPServer::new();
    server.register_handler(PProfHandler::default());
    server.register_handler(MetricsHTTPHandler::default());
    Box::new(server)
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use crate::http::{HTTP_SERVICE, HTTPServer};

    #[tokio::test]
    async fn http_service_test() {
        HTTP_SERVICE.start(20019);
        tokio::time::sleep(Duration::from_secs(1000)).await;
    }
}