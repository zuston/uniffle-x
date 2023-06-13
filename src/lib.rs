
pub mod proto;
pub mod app;
pub mod store;
pub mod grpc;
pub mod error;
pub mod config;
pub mod metric;
pub mod util;
pub mod readable_size;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use anyhow::Result;
use log::info;
use tokio::runtime::Runtime;
use tonic::transport::Server;
use crate::app::AppManager;
use crate::grpc::DefaultShuffleServer;
use crate::proto::uniffle::shuffle_server_server::ShuffleServerServer;

pub async fn start_datanode(config: config::Config) -> Result<()> {
    // implement server startup
    tokio::spawn(async move {
        let app_manager_ref = AppManager::get_ref(config.clone());
        let rpc_port = config.grpc_port.unwrap_or(19999);
        info!("Starting GRpc server with port:[{}] ......", rpc_port);
        let shuffle_server = DefaultShuffleServer::from(app_manager_ref);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), rpc_port as u16);
        let service = ShuffleServerServer::new(shuffle_server).max_decoding_message_size(usize::MAX).max_encoding_message_size(usize::MAX);
        let _ = Server::builder()
            .add_service(service)
            .serve(addr)
            .await;
    });
    Ok(())
}
