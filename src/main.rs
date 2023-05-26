extern crate core;

use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;
use crate::app::{AppManager, AppManagerRef};
use crate::grpc::DefaultShuffleServer;
use crate::proto::uniffle::shuffle_server_server::ShuffleServerServer;
use anyhow::{Result, anyhow};
use log::info;
use tracing_subscriber::fmt::format;
use crate::config::Config;
use crate::proto::uniffle::coordinator_server_client::CoordinatorServerClient;
use crate::proto::uniffle::{ShuffleServerHeartBeatRequest, ShuffleServerId, StatusCode};

pub mod proto;
pub mod app;
pub mod store;
pub mod grpc;
mod error;
mod config;

fn get_local_ip() -> Result<IpAddr, std::io::Error> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    let local_addr = socket.local_addr()?;
    Ok(local_addr.ip())
}

async fn schedule_coordinator_report(app_manager: AppManagerRef, coordinator_quorum: Vec<String>, grpc_port: i32) -> anyhow::Result<()> {
    tokio::spawn(async move {

        let ip = get_local_ip().unwrap().to_string();

        info!("ip: {}", ip.clone());

        let shuffle_server_id = ShuffleServerId {
            id: format!("{}-{}", ip.clone(), grpc_port),
            ip,
            port: grpc_port,
            netty_port: 0
        };

        let mut client_1 = CoordinatorServerClient::connect(format!("http://{}", coordinator_quorum.get(0).unwrap())).await.unwrap();

        // let mut multi_coordinator_clients = vec![
        // ];

        // let mut multi_coordinator_clients =
        //     coordinator_quorum.into_iter().map(async |quorum| CoordinatorServerClient::connect(format!("http://{}", quorum)).await?).collect();

        loop {
            // todo: add interval as config var
            tokio::time::sleep(Duration::from_secs(10)).await;

            let request = tonic::Request::new(ShuffleServerHeartBeatRequest {
                server_id: Some(shuffle_server_id.clone()),
                used_memory: 0,
                pre_allocated_memory: 0,
                available_memory: 1024 * 1024 * 1024 * 10,
                event_num_in_flush: 0,
                tags: vec!["ss_v4".to_string()],
                is_healthy: Some(true),
                status: 0,
                storage_info: Default::default()
            });
            client_1.heartbeat(request).await.unwrap();
        }
    });

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let config = Config::create_from_env();
    let rpc_port = config.grpc_port.unwrap_or(19999);
    let coordinator_quorum = config.coordinator_quorum.clone();
    let appManagerRef = AppManager::get_ref(config);
    let _ = schedule_coordinator_report(appManagerRef.clone(), coordinator_quorum, rpc_port).await;

    info!("Starting GRpc server with port:[{}] ......", rpc_port);
    let addr = format!("[::1]:{}", rpc_port).parse()?;
    let shuffle_server = DefaultShuffleServer::from(appManagerRef);
    Server::builder()
        .add_service(ShuffleServerServer::new(shuffle_server))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::get_local_ip;
    use crate::proto::uniffle::shuffle_server_client::ShuffleServerClient;

    #[test]
    fn get_local_ip_test() {
        let ip = get_local_ip().unwrap();
        println!("{}", ip.to_string());
    }

    // #[test]
    // async fn put_get_in_local_grpc() {
    //     let client = ShuffleServerClient::connect("http://127.0.0.0:19999").await?;
    // }
}