use bytes::{BufMut, BytesMut};
use log::{error, info};
use toml::Value::String;
use tonic::{Request, Response, Status};
use crate::app::{GetBlocksContext, PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext, ReportBlocksContext, RequireBufferContext, WritingViewContext};
use crate::{AppManagerRef, StatusCode};
use crate::proto::uniffle::shuffle_server_server::ShuffleServer;
use crate::proto::uniffle::{AppHeartBeatRequest, AppHeartBeatResponse, FinishShuffleRequest, FinishShuffleResponse, GetLocalShuffleDataRequest, GetLocalShuffleDataResponse, GetLocalShuffleIndexRequest, GetLocalShuffleIndexResponse, GetMemoryShuffleDataRequest, GetMemoryShuffleDataResponse, GetShuffleResultForMultiPartRequest, GetShuffleResultForMultiPartResponse, GetShuffleResultRequest, GetShuffleResultResponse, ReportShuffleResultRequest, ReportShuffleResultResponse, RequireBufferRequest, RequireBufferResponse, SendShuffleDataRequest, SendShuffleDataResponse, ShuffleCommitRequest, ShuffleCommitResponse, ShuffleRegisterRequest, ShuffleRegisterResponse, ShuffleUnregisterRequest, ShuffleUnregisterResponse};
use crate::proto::uniffle::coordinator_server_server::CoordinatorServer;
use crate::store::{PartitionedData, PartitionedDataBlock, ResponseData, ResponseDataIndex};

pub struct DefaultShuffleServer {
    app_manager_ref: AppManagerRef
}

impl DefaultShuffleServer {
    pub fn from(appManagerRef: AppManagerRef) -> DefaultShuffleServer {
        DefaultShuffleServer {
            app_manager_ref: appManagerRef
        }
    }
}

#[tonic::async_trait]
impl ShuffleServer for DefaultShuffleServer {
    async fn register_shuffle(&self, request: Request<ShuffleRegisterRequest>) -> Result<Response<ShuffleRegisterResponse>, Status> {
        let inner = request.into_inner();
        let status = match self.app_manager_ref.register(inner.app_id, inner.shuffle_id) {
            Ok(_) => 0,
            _ => 6
        };
        Ok(
            Response::new(ShuffleRegisterResponse {
                status,
                ret_msg: "".to_string()
            })
        )
    }

    async fn unregister_shuffle(&self, request: Request<ShuffleUnregisterRequest>) -> Result<Response<ShuffleUnregisterResponse>, Status> {
        info!("Accepted unregister shuffle info....");
        Ok(
            Response::new(ShuffleUnregisterResponse {
                status: 0,
                ret_msg: "".to_string()
            })
        )
    }

    async fn send_shuffle_data(&self, request: Request<SendShuffleDataRequest>) -> Result<Response<SendShuffleDataResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id: i32 = req.shuffle_id;

        let app_option = self.app_manager_ref.get_app(&app_id);

        if app_option.is_none() {
            return Ok(
                Response::new(SendShuffleDataResponse {
                    status: 1,
                    ret_msg: "".to_string()
                })
            );
        }

        let mut app = app_option.unwrap();

        let blocks: Vec<PartitionedDataBlock> = vec![];
        for shuffle_data in req.shuffle_data {
            let data: PartitionedData = shuffle_data.into();
            let partitioned_blocks = data.blocks;
            let ctx = WritingViewContext {
                uid: PartitionedUId {
                    app_id: app_id.clone(),
                    shuffle_id,
                    partition_id: data.partitionId
                },
                data_blocks: partitioned_blocks
            };

            match app.insert(ctx).await {
                Err(error) => error!("Errors on putting data, app_id: {}", app_id.clone()),
                _ => continue
            }
        }

        // todo: if the data sent failed, return the error code to client side
        Ok(
            Response::new(SendShuffleDataResponse {
                status: 0,
                ret_msg: "".to_string()
            })
        )
    }

    async fn get_local_shuffle_index(&self, request: Request<GetLocalShuffleIndexRequest>) -> Result<Response<GetLocalShuffleIndexResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id: i32 = req.shuffle_id;
        let partition_id = req.partition_id;
        let partition_num = req.partition_num;
        let partition_per_range = req.partition_num_per_range;

        let app_option = self.app_manager_ref.get_app(&app_id);

        if app_option.is_none() {
            return Ok(
                Response::new(GetLocalShuffleIndexResponse {
                    index_data: Default::default(),
                    status: 1,
                    ret_msg: "Partition not found".to_string(),
                    data_file_len: 0
                })
            );
        }

        let app = app_option.unwrap();

        let data_index_wrapper = app.list_index(ReadingIndexViewContext {
            partition_id: PartitionedUId::from(app_id, shuffle_id, partition_id)
        }).await.unwrap();

        match data_index_wrapper {
            ResponseDataIndex::local(data_index) => {
                Ok(
                    Response::new(GetLocalShuffleIndexResponse {
                        index_data: data_index.index_data,
                        status: 0,
                        ret_msg: "".to_string(),
                        data_file_len: data_index.data_file_len
                    })
                )
            },
            _ => Ok(
                Response::new(GetLocalShuffleIndexResponse {
                    index_data: Default::default(),
                    status: 1,
                    ret_msg: "index file not found".to_string(),
                    data_file_len: 0
                })
            )
        }
    }

    async fn get_local_shuffle_data(&self, request: Request<GetLocalShuffleDataRequest>) -> Result<Response<GetLocalShuffleDataResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id: i32 = req.shuffle_id;
        let partition_id = req.partition_id;

        let app = self.app_manager_ref.get_app(&app_id).unwrap();
        let data = app.select(ReadingViewContext {
            uid: PartitionedUId {
                app_id,
                shuffle_id,
                partition_id
            },
            reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(
                req.offset, req.length as i64
            )
        }).await.unwrap();
        
        Ok(Response::new(GetLocalShuffleDataResponse {
            data: data.from_local(),
            status: 0,
            ret_msg: "".to_string()
        }))
    }

    async fn get_memory_shuffle_data(&self, request: Request<GetMemoryShuffleDataRequest>) -> Result<Response<GetMemoryShuffleDataResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id: i32 = req.shuffle_id;
        let partition_id = req.partition_id;

        let app = self.app_manager_ref.get_app(&app_id).unwrap();
        let data = app.select(ReadingViewContext {
            uid: PartitionedUId {
                app_id,
                shuffle_id,
                partition_id
            },
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                req.last_block_id, req.read_buffer_size as i64
            )
        }).await.unwrap();

        let data = data.from_memory();

        Ok(Response::new(GetMemoryShuffleDataResponse {
            shuffle_data_block_segments: data.shuffle_data_block_segments.into_iter().map(|x| x.into()).collect(),
            data: data.data,
            status: 0,
            ret_msg: "".to_string()
        }))
    }

    async fn commit_shuffle_task(&self, request: Request<ShuffleCommitRequest>) -> Result<Response<ShuffleCommitResponse>, Status> {
        panic!("It has not been supported.")
    }

    async fn report_shuffle_result(&self, request: Request<ReportShuffleResultRequest>) -> Result<Response<ReportShuffleResultResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;
        let partition_to_block_ids = req.partition_to_block_ids;

        let app = self.app_manager_ref.get_app(&app_id).unwrap();

        for partition_to_block_id in partition_to_block_ids {
            let partition_id = partition_to_block_id.partition_id;
            let ctx = ReportBlocksContext {
                uid: PartitionedUId {
                    app_id: app_id.clone(),
                    shuffle_id,
                    partition_id
                },
                blocks: partition_to_block_id.block_ids
            };
            let _ = app.report_block_ids(ctx).await;
        }

        Ok(Response::new(ReportShuffleResultResponse {
            status: 0,
            ret_msg: "".to_string()
        }))
    }

    async fn get_shuffle_result(&self, request: Request<GetShuffleResultRequest>) -> Result<Response<GetShuffleResultResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;
        let partition_id = req.partition_id;

        let app = self.app_manager_ref.get_app(&app_id).unwrap();
        let data = app.get_block_ids(GetBlocksContext {
            uid: PartitionedUId {
                app_id,
                shuffle_id,
                partition_id
            }
        }).await.unwrap();

        Ok(Response::new(GetShuffleResultResponse {
            status: 0,
            ret_msg: "".to_string(),
            serialized_bitmap: data
        }))
    }

    async fn get_shuffle_result_for_multi_part(&self, request: Request<GetShuffleResultForMultiPartRequest>) -> Result<Response<GetShuffleResultForMultiPartResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;

        let mut bytes_mut = BytesMut::new();
        for partition_id in req.partitions {
            let app = self.app_manager_ref.get_app(&app_id).unwrap();
            let data = app.get_block_ids(GetBlocksContext {
                uid: PartitionedUId {
                    app_id: app_id.clone(),
                    shuffle_id,
                    partition_id
                }
            }).await.unwrap();
            bytes_mut.put(data);
        }


        Ok(Response::new(GetShuffleResultForMultiPartResponse {
            status: 0,
            ret_msg: "".to_string(),
            serialized_bitmap: bytes_mut.freeze()
        }))
    }

    async fn finish_shuffle(&self, request: Request<FinishShuffleRequest>) -> Result<Response<FinishShuffleResponse>, Status> {
        info!("Accepted unregister shuffle info....");
        Ok(
            Response::new(FinishShuffleResponse {
                status: 0,
                ret_msg: "".to_string()
            })
        )
    }

    async fn require_buffer(&self, request: Request<RequireBufferRequest>) -> Result<Response<RequireBufferResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;

        let app = self.app_manager_ref.get_app(&app_id).unwrap();
        let (is_ok, id) = app.require_buffer(RequireBufferContext {
            uid: PartitionedUId {
                app_id,
                shuffle_id,
                // ignore this.
                partition_id: 1
            },
            size: req.require_size as i64
        }).await.unwrap();

        let mut status_code = 0;
        if !is_ok {
            // 2 = No buffer
            status_code = 2;
        }

        Ok(Response::new(RequireBufferResponse {
            require_buffer_id: id,
            status: status_code,
            ret_msg: "".to_string()
        }))
    }

    async fn app_heartbeat(&self, request: Request<AppHeartBeatRequest>) -> Result<Response<AppHeartBeatResponse>, Status> {
        info!("Accepted heartbeat for app: {:#?}", request.into_inner().app_id);
        Ok(Response::new(AppHeartBeatResponse {
            status: 0,
            ret_msg: "".to_string()
        }))
    }
}