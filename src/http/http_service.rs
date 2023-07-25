use crate::error::DatanodeError;

use poem::endpoint::make_sync;
use poem::error::ResponseError;
use poem::http::StatusCode;
use poem::listener::TcpListener;
use poem::{get, Route, RouteMethod, Server};

use std::sync::Mutex;

use crate::http::{HTTPServer, Handler};

impl ResponseError for DatanodeError {
    fn status(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

struct IndexPageHandler {}
impl Handler for IndexPageHandler {
    fn get_route_method(&self) -> RouteMethod {
        get(make_sync(|_| "Hello uniffle server"))
    }

    fn get_route_path(&self) -> String {
        "/".to_string()
    }
}

pub struct PoemHTTPServer {
    handlers: Mutex<Vec<Box<dyn Handler>>>,
}

unsafe impl Send for PoemHTTPServer {}
unsafe impl Sync for PoemHTTPServer {}

impl PoemHTTPServer {
    pub fn new() -> Self {
        let handlers: Vec<Box<dyn Handler>> = vec![Box::new(IndexPageHandler {})];
        Self {
            handlers: Mutex::new(handlers),
        }
    }
}

impl HTTPServer for PoemHTTPServer {
    fn start(&self, port: u16) {
        let mut app = Route::new();
        let handlers = self.handlers.lock().unwrap();
        for handler in handlers.iter() {
            app = app.at(handler.get_route_path(), handler.get_route_method());
        }
        tokio::spawn(async move {
            let _ = Server::new(TcpListener::bind(format!("0.0.0.0:{}", port)))
                .name("uniffle-server-http-service")
                .run(app)
                .await;
        });
    }

    fn register_handler(&self, handler: impl Handler + 'static) {
        let mut handlers = self.handlers.lock().unwrap();
        handlers.push(Box::new(handler));
    }
}
