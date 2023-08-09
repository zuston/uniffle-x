// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![feature(impl_trait_in_assoc_type)]

pub mod app;
pub mod await_tree;
pub mod config;
pub mod error;
pub mod grpc;
pub mod http;
mod mem_allocator;
pub mod metric;
pub mod proto;
pub mod readable_size;
pub mod store;
pub mod util;

use crate::app::AppManager;
use crate::grpc::DefaultShuffleServer;
use crate::http::{HTTPServer, HTTP_SERVICE};
use crate::metric::configure_metric_service;
use crate::proto::uniffle::shuffle_server_server::ShuffleServerServer;
use crate::util::gen_worker_uid;
use anyhow::Result;
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tonic::transport::Server;

pub async fn start_uniffle_worker(config: config::Config) -> Result<()> {
    let rpc_port = config.grpc_port.unwrap_or(19999);
    let worker_uid = gen_worker_uid(rpc_port);
    let metric_config = config.metrics.clone();
    configure_metric_service(&metric_config, worker_uid.clone());
    // start the http monitor service
    let http_port = config.http_monitor_service_port.unwrap_or(20010);
    HTTP_SERVICE.start(http_port);
    // implement server startup
    tokio::spawn(async move {
        let app_manager_ref = AppManager::get_ref(config.clone());
        let rpc_port = config.grpc_port.unwrap_or(19999);
        info!("Starting GRpc server with port:[{}] ......", rpc_port);
        let shuffle_server = DefaultShuffleServer::from(app_manager_ref);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), rpc_port as u16);
        let service = ShuffleServerServer::new(shuffle_server)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        let _ = Server::builder().add_service(service).serve(addr).await;
    });
    Ok(())
}
