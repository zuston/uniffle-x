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
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, fmt, Registry};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use crate::config::{Config, LogConfig, MetricsConfig, RotationConfig};
use crate::metric::start_metric_service;
use crate::proto::uniffle::coordinator_server_client::CoordinatorServerClient;
use crate::proto::uniffle::{ShuffleServerHeartBeatRequest, ShuffleServerId, StatusCode};
use crate::store::ResponseData::mem;
use crate::util::get_local_ip;

pub mod proto;
pub mod app;
pub mod store;
pub mod grpc;
mod error;
mod config;
mod metric;
mod util;
mod readable_size;
mod await_tree;

const DEFAULT_SHUFFLE_SERVER_TAG: &str = "ss_v4";

async fn schedule_coordinator_report(
    app_manager: AppManagerRef,
    coordinator_quorum: Vec<String>,
    grpc_port: i32,
    tags: Vec<String>,
    datanode_uid: String) -> anyhow::Result<()> {

    tokio::spawn(async move {

        let ip = get_local_ip().unwrap().to_string();

        info!("machine ip: {}", ip.clone());

        let shuffle_server_id = ShuffleServerId {
            id: datanode_uid,
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
            all_tags.push(DEFAULT_SHUFFLE_SERVER_TAG.to_string());
            all_tags.extend_from_slice(&*tags);

            let healthy = app_manager.store_is_healthy().await.unwrap_or(false);
            let memory_snapshot = app_manager.store_memory_snapshot().await.unwrap_or((0, 0, 0).into());
            let memory_spill_event_num = app_manager.store_memory_spill_event_num().unwrap_or(0) as i32;

            let heartbeat_req = ShuffleServerHeartBeatRequest {
                server_id: Some(shuffle_server_id.clone()),
                used_memory: memory_snapshot.get_used(),
                pre_allocated_memory: memory_snapshot.get_allocated(),
                available_memory: memory_snapshot.get_capacity() - memory_snapshot.get_used() - memory_snapshot.get_allocated(),
                event_num_in_flush: memory_spill_event_num,
                tags: all_tags,
                is_healthy: Some(healthy),
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

fn init_log(log: &LogConfig) -> WorkerGuard {
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
        .with_line_number(true)
        .with_writer(non_blocking);

    Registry::default()
        .with(env_filter)
        .with(formatting_layer)
        .with(file_layer)
        .init();

    // Note: _guard is a WorkerGuard which is returned by tracing_appender::non_blocking to
    // ensure buffered logs are flushed to their output in the case of abrupt terminations of a process.
    // See WorkerGuard module for more details.
    _guard
}

fn gen_datanode_uid(grpc_port: i32) -> String {
    let ip = get_local_ip().unwrap().to_string();
    format!("{}-{}", ip.clone(), grpc_port)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::create_from_env();

    // init log
    let log_config = &config.log.clone().unwrap_or(Default::default());
    let _guard = init_log(log_config);

    let rpc_port = config.grpc_port.unwrap_or(19999);
    let datanode_uid = gen_datanode_uid(rpc_port);

    // start metric service to expose http api
    start_metric_service(&config.metrics, datanode_uid.clone());

    let coordinator_quorum = config.coordinator_quorum.clone();
    let tags = config.tags.clone().unwrap_or(vec![]);
    let app_manager_ref = AppManager::get_ref(config);
    let _ = schedule_coordinator_report(
        app_manager_ref.clone(),
        coordinator_quorum,
        rpc_port,
        tags,
        datanode_uid).await;

    info!("Starting GRpc server with port:[{}] ......", rpc_port);
    let shuffle_server = DefaultShuffleServer::from(app_manager_ref);
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), rpc_port as u16);
    let service = ShuffleServerServer::new(shuffle_server).max_decoding_message_size(usize::MAX).max_encoding_message_size(usize::MAX);
    Server::builder()
        .add_service(service)
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