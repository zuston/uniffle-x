mod await_tree;
mod http_service;
mod metrics;
mod pprof;
mod jeprof;

use crate::http::await_tree::AwaitTreeHandler;
use crate::http::http_service::PoemHTTPServer;
use crate::http::metrics::MetricsHTTPHandler;
use crate::http::pprof::PProfHandler;
use crate::http::jeprof::JeProfHandler;
use lazy_static::lazy_static;
use poem::RouteMethod;

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
    server.register_handler(AwaitTreeHandler::default());
    server.register_handler(JeProfHandler::default());
    Box::new(server)
}
