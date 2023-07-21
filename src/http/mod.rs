mod pprof;
mod http_service;
mod metrics;

use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;
use poem::RouteMethod;
use crate::http::http_service::PoemHTTPServer;
use crate::http::metrics::MetricsHTTPHandler;
use crate::http::pprof::PProfHandler;

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