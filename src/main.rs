
use tonic::transport::Server;
use crate::app::{AppManager, AppManagerRef};
use crate::grpc::DefaultShuffleServer;
use crate::proto::uniffle::shuffle_server_server::ShuffleServerServer;
use anyhow::{Result, anyhow};

pub mod proto;
pub mod app;
pub mod store;
pub mod grpc;
mod error;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let appManagerRef = AppManager::get_ref();

    let addr = "[::1]:19999".parse()?;
    let shuffle_server = DefaultShuffleServer::from(appManagerRef);
    Server::builder()
        .add_service(ShuffleServerServer::new(shuffle_server))
        .serve(addr)
        .await?;

    Ok(())
}