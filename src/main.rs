extern crate core;

use std::borrow::BorrowMut;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Server};
use crate::app::{AppManager, AppManagerRef};
use crate::grpc::DefaultShuffleServer;
use crate::proto::uniffle::shuffle_server_server::ShuffleServerServer;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use log::info;
use tracing_subscriber::{EnvFilter, fmt, Registry};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use crate::config::{Config, LogConfig, RotationConfig};
use crate::metric::start_metric_service;
use crate::proto::uniffle::coordinator_server_client::CoordinatorServerClient;
use crate::proto::uniffle::{ShuffleServerHeartBeatRequest, ShuffleServerId, StatusCode};
use crate::util::get_local_ip;

pub mod proto;
pub mod app;
pub mod store;
pub mod grpc;
mod error;
mod config;
mod metric;
mod util;

async fn schedule_coordinator_report(
    app_manager: AppManagerRef,
    coordinator_quorum: Vec<String>,
    grpc_port: i32,
    tags: Vec<String>) -> anyhow::Result<()> {

    tokio::spawn(async move {

        let ip = get_local_ip().unwrap().to_string();

        info!("machine ip: {}", ip.clone());

        let shuffle_server_id = ShuffleServerId {
            id: format!("{}-{}", ip.clone(), grpc_port),
            ip,
            port: grpc_port,
            netty_port: 0
        };

        let mut multi_coordinator_clients: Vec<CoordinatorServerClient<Channel>> =
        futures::future::try_join_all(
            coordinator_quorum.iter().map(|quorum| CoordinatorServerClient::connect(format!("http://{}", quorum)))
        ).await.unwrap();

        loop {
            // todo: add interval as config var
            tokio::time::sleep(Duration::from_secs(10)).await;

            let mut all_tags = vec![];
            all_tags.push("ss_v4".to_string());
            all_tags.extend_from_slice(&*tags);

            let heartbeat_req = ShuffleServerHeartBeatRequest {
                server_id: Some(shuffle_server_id.clone()),
                used_memory: 0,
                pre_allocated_memory: 0,
                available_memory: 1024 * 1024 * 1024 * 10,
                event_num_in_flush: 0,
                tags: all_tags,
                is_healthy: Some(true),
                status: 0,
                storage_info: Default::default()
            };

            // It must use the 0..len to avoid borrow check in loop.
            for idx in 0..multi_coordinator_clients.len() {
                let client = multi_coordinator_clients.get_mut(idx).unwrap();
                let _ = client.heartbeat(tonic::Request::new(heartbeat_req.clone())).await;
            }
        }
    });

    Ok(())
}

const LOG_FILE_NAME: &str = "uniffle-datanode.log";

fn init_log(log: &LogConfig) {
    let file_appender = match log.rotation {
        RotationConfig::Hourly => tracing_appender::rolling::hourly(&log.path, LOG_FILE_NAME),
        RotationConfig::Daily => tracing_appender::rolling::daily(&log.path, LOG_FILE_NAME),
        RotationConfig::Never => tracing_appender::rolling::never(&log.path, LOG_FILE_NAME),
    };

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let formatting_layer = fmt::layer().pretty().with_writer(std::io::stderr);

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_writer(non_blocking);

    Registry::default()
        .with(env_filter)
        .with(formatting_layer)
        .with(file_layer)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::create_from_env();

    // init log
    let log_config = &config.log.clone().unwrap_or(Default::default());
    init_log(log_config);

    // start metric service to expose http api
    let metric_http_port = config.metric_http_port.unwrap_or(19998);
    start_metric_service(metric_http_port);

    let rpc_port = config.grpc_port.unwrap_or(19999);
    let coordinator_quorum = config.coordinator_quorum.clone();
    let tags = config.tags.clone().unwrap_or(vec![]);
    let app_manager_ref = AppManager::get_ref(config);
    let _ = schedule_coordinator_report(
        app_manager_ref.clone(),
        coordinator_quorum,
        rpc_port,
        tags).await;

    info!("Starting GRpc server with port:[{}] ......", rpc_port);
    let shuffle_server = DefaultShuffleServer::from(app_manager_ref);
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), rpc_port as u16);
    Server::builder()
        .add_service(ShuffleServerServer::new(shuffle_server))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use std::borrow::BorrowMut;
    use std::cell::RefCell;
    use std::rc::Rc;
    use tokio_stream::StreamExt;
    use crate::get_local_ip;
    use crate::proto::uniffle::shuffle_server_client::ShuffleServerClient;
    use anyhow::Result;

    #[test]
    fn get_local_ip_test() {
        let ip = get_local_ip().unwrap();
        println!("{}", ip.to_string());
    }
}