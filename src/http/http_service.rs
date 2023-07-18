use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use warp::Filter;
use log::info;

use crate::http::metrics::metrics_handler;
use crate::http::pprof::{PProfRequest, pprof_handler};

pub trait HTTPServer: Send {
    fn start(&mut self);
}

pub fn new_server(port: u16) -> impl HTTPServer {
    WarpServer::new(port)
}

struct WarpServer {
    port: u16,
}

impl WarpServer {
    fn new(port: u16) -> Self {
        WarpServer {
            port,
        }
    }
}

impl HTTPServer for WarpServer {
    fn start(&mut self) {
        let hello_world = warp::path::end().map(|| "Hello uniffle server");

        // todo: maybe we should replace warp with other rest framework, it's super weired to
        //   support conditional routing.
        let metrics_route = warp::get()
            .and(warp::path("metrics"))
            .and_then(metrics_handler);

        let pprof_route = warp::get()
            .and(warp::path!("debug" / "pprof" / "profile"))
            .and(warp::query::<PProfRequest>())
            .and_then(pprof_handler);

        let routes = hello_world.or(metrics_route).or(pprof_route);
        let port = self.port;
        info!("Starting http service with port:[{}] ......", port);
        tokio::spawn(async move {
            warp::serve(routes)
                .run(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port))
                .await;
        });
    }
}