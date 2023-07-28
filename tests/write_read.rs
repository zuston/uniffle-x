#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bytes::{Buf, Bytes, BytesMut};
    use datanode::config::{
        Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig, MetricsConfig,
        StorageType,
    };
    use datanode::proto::uniffle::shuffle_server_client::ShuffleServerClient;
    use datanode::proto::uniffle::{
        GetLocalShuffleDataRequest, GetLocalShuffleIndexRequest, GetMemoryShuffleDataRequest,
        RequireBufferRequest, SendShuffleDataRequest, ShuffleBlock, ShuffleData,
        ShuffleRegisterRequest,
    };
    use datanode::start_datanode;

    use std::time::Duration;
    use tonic::transport::Channel;

    fn create_mocked_config(grpc_port: i32, capacity: String, local_data_path: String) -> Config {
        Config {
            memory_store: Some(MemoryStoreConfig::new(capacity)),
            localfile_store: Some(LocalfileStoreConfig {
                data_paths: vec![local_data_path],
                healthy_check_min_disks: Some(0),
                disk_high_watermark: None,
                disk_low_watermark: None,
                disk_max_concurrency: None,
            }),
            hybrid_store: Some(HybridStoreConfig::new(0.9, 0.5, None)),
            hdfs_store: None,
            store_type: Some(StorageType::MEMORY_LOCALFILE),
            metrics: Some(MetricsConfig {
                push_gateway_endpoint: None,
                push_interval_sec: None,
            }),
            grpc_port: Some(grpc_port),
            coordinator_quorum: vec![],
            tags: None,
            log: None,
            app_heartbeat_timeout_min: None,
            huge_partition_marked_threshold: None,
            huge_partition_memory_max_used_percent: None,
            http_monitor_service_port: None,
        }
    }

    async fn get_data_from_remote(
        _client: &ShuffleServerClient<Channel>,
        _app_id: &str,
        _shuffle_id: i32,
        _partitions: Vec<i32>,
    ) {
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn write_read_test() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("test_write_read").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        println!("created the temp file path: {}", &temp_path);

        let port = 21101;
        let config = create_mocked_config(port, "1G".to_string(), temp_path);
        let _ = start_datanode(config).await;

        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut client =
            ShuffleServerClient::connect(format!("http://{}:{}", "0.0.0.0", port)).await?;

        let app_id = "write_read_test-app-id".to_string();

        let register_response = client
            .register_shuffle(ShuffleRegisterRequest {
                app_id: app_id.clone(),
                shuffle_id: 0,
                partition_ranges: vec![],
                remote_storage: None,
                user: "".to_string(),
                shuffle_data_distribution: 1,
                max_concurrency_per_partition_to_write: 10,
            })
            .await?
            .into_inner();
        assert_eq!(register_response.status, 0);

        let mut all_bytes_data = BytesMut::new();
        let mut block_ids = vec![];

        let batch_size = 100;

        for idx in 0..batch_size {
            block_ids.push(idx as i64);

            let data = b"hello world";
            let len = data.len();

            all_bytes_data.extend_from_slice(data);

            let buffer_required_resp = client
                .require_buffer(RequireBufferRequest {
                    require_size: len as i32,
                    app_id: app_id.clone(),
                    shuffle_id: 0,
                    partition_ids: vec![],
                })
                .await?
                .into_inner();

            assert_eq!(0, buffer_required_resp.status);

            let response = client
                .send_shuffle_data(SendShuffleDataRequest {
                    app_id: app_id.clone(),
                    shuffle_id: 0,
                    require_buffer_id: buffer_required_resp.require_buffer_id,
                    shuffle_data: vec![ShuffleData {
                        partition_id: idx,
                        block: vec![ShuffleBlock {
                            block_id: idx as i64,
                            length: len as i32,
                            uncompress_length: 0,
                            crc: 0,
                            data: Bytes::copy_from_slice(data),
                            task_attempt_id: 0,
                        }],
                    }],
                    timestamp: 0,
                })
                .await?;

            let response = response.into_inner();
            assert_eq!(0, response.status);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut accepted_block_ids = vec![];
        let mut accepted_data_bytes = BytesMut::new();
        // firstly. read from the memory

        for idx in 0..batch_size {
            let response_data = client
                .get_memory_shuffle_data(GetMemoryShuffleDataRequest {
                    app_id: app_id.clone(),
                    shuffle_id: 0,
                    partition_id: idx,
                    last_block_id: -1,
                    read_buffer_size: 10000000,
                    timestamp: 0,
                    serialized_expected_task_ids_bitmap: Default::default(),
                })
                .await?;
            let response = response_data.into_inner();
            let segments = response.shuffle_data_block_segments;
            for segment in segments {
                accepted_block_ids.push(segment.block_id)
            }
            let data = response.data;
            accepted_data_bytes.extend_from_slice(&data);
        }

        // secondly, read from the localfile
        for idx in 0..batch_size {
            let local_index_data = client
                .get_local_shuffle_index(GetLocalShuffleIndexRequest {
                    app_id: app_id.clone(),
                    shuffle_id: 0,
                    partition_id: idx,
                    partition_num_per_range: 1,
                    partition_num: 0,
                })
                .await?;

            let mut bytes = local_index_data.into_inner().index_data;
            if bytes.is_empty() {
                continue;
            }
            // index_bytes_holder.put_i64(next_offset);
            // index_bytes_holder.put_i32(length);
            // index_bytes_holder.put_i32(uncompress_len);
            // index_bytes_holder.put_i64(crc);
            // index_bytes_holder.put_i64(block_id);
            // index_bytes_holder.put_i64(task_attempt_id);
            bytes.get_i64();
            let len = bytes.get_i32();
            bytes.get_i32();
            bytes.get_i64();
            let id = bytes.get_i64();
            accepted_block_ids.push(id);

            let partitioned_local_data = client
                .get_local_shuffle_data(GetLocalShuffleDataRequest {
                    app_id: app_id.clone(),
                    shuffle_id: 0,
                    partition_id: idx,
                    partition_num_per_range: 0,
                    partition_num: 0,
                    offset: 0,
                    length: len,
                    timestamp: 0,
                })
                .await?;
            accepted_data_bytes.extend_from_slice(&partitioned_local_data.into_inner().data);
        }

        // check the block ids
        assert_eq!(batch_size as usize, accepted_block_ids.len());
        assert_eq!(block_ids, accepted_block_ids);

        // check the shuffle data
        assert_eq!(all_bytes_data.freeze(), accepted_data_bytes.freeze());

        Ok(())
    }
}
