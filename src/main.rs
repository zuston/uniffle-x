extern crate core;

use std::rc::Rc;
use std::sync::Arc;
use tonic::transport::Server;
use crate::app::{AppManager, AppManagerRef};
use crate::grpc::DefaultShuffleServer;
use crate::proto::uniffle::shuffle_server_server::ShuffleServerServer;
use anyhow::{Result, anyhow};
use log::info;
use crate::config::Config;

pub mod proto;
pub mod app;
pub mod store;
pub mod grpc;
mod error;
mod config;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let config = Config::create_from_env();
    let rpc_port = config.grpc_port.unwrap_or(19999);

    info!("Starting GRpc server with port:[{}] ......", rpc_port);

    let appManagerRef = AppManager::get_ref(config);

    let addr = format!("[::1]:{}", rpc_port).parse()?;
    let shuffle_server = DefaultShuffleServer::from(appManagerRef);
    Server::builder()
        .add_service(ShuffleServerServer::new(shuffle_server))
        .serve(addr)
        .await?;

    Ok(())
}