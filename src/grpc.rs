use std::ops::Deref;
use bytes::{BufMut, BytesMut};
use futures::future::err;
use log::{debug, error, info, warn};
use toml::Value::String;
use tonic::{Request, Response, Status};
use crate::app::{AppManagerRef, GetBlocksContext, PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext, ReportBlocksContext, RequireBufferContext, WritingViewContext};
use crate::proto::uniffle::shuffle_server_server::ShuffleServer;
use crate::proto::uniffle::{AppHeartBeatRequest, AppHeartBeatResponse, FinishShuffleRequest, FinishShuffleResponse, GetLocalShuffleDataRequest, GetLocalShuffleDataResponse, GetLocalShuffleIndexRequest, GetLocalShuffleIndexResponse, GetMemoryShuffleDataRequest, GetMemoryShuffleDataResponse, GetShuffleResultForMultiPartRequest, GetShuffleResultForMultiPartResponse, GetShuffleResultRequest, GetShuffleResultResponse, ReportShuffleResultRequest, ReportShuffleResultResponse, RequireBufferRequest, RequireBufferResponse, SendShuffleDataRequest, SendShuffleDataResponse, ShuffleCommitRequest, ShuffleCommitResponse, ShuffleRegisterRequest, ShuffleRegisterResponse, ShuffleUnregisterRequest, ShuffleUnregisterResponse};
use crate::proto::uniffle::coordinator_server_server::CoordinatorServer;
use crate::store::{PartitionedData, PartitionedDataBlock, ResponseData, ResponseDataIndex};

enum StatusCode {
    SUCCESS = 0,
    DOUBLE_REGISTER = 1,
    NO_BUFFER = 2,
    INVALID_STORAGE = 3,
    NO_REGISTER = 4,
    NO_PARTITION = 5,
    INTERNAL_ERROR = 6,
    TIMEOUT = 7,
}

impl Into<i32> for StatusCode {
    fn into(self) -> i32 {
        self as i32
    }
}

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
        let status =
            self.app_manager_ref
                .register(inner.app_id, inner.shuffle_id)
                .map_or(StatusCode::INTERNAL_ERROR, |_| StatusCode::SUCCESS)
                .into();
        Ok(
            Response::new(ShuffleRegisterResponse {
                status,
                ret_msg: "".to_string()
            })
        )
    }

    async fn unregister_shuffle(&self, request: Request<ShuffleUnregisterRequest>) -> Result<Response<ShuffleUnregisterResponse>, Status> {
        // todo: implement shuffle level deletion
        info!("Accepted unregister shuffle info....");
        Ok(
            Response::new(ShuffleUnregisterResponse {
                status: StatusCode::SUCCESS.into(),
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
                    status: StatusCode::NO_REGISTER.into(),
                    ret_msg: "The app is not found".to_string()
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

            let inserted = app.insert(ctx).await;
            if inserted.is_err() {
                let err = format!("Errors on putting data. app_id: {}, err: {:?}", &app_id, inserted.err());
                error!("{}", &err);
                return Ok(
                    Response::new(SendShuffleDataResponse {
                        status: StatusCode::INTERNAL_ERROR.into(),
                        ret_msg: err,
                    })
                )
            }
        }

        Ok(
            Response::new(SendShuffleDataResponse {
                status: StatusCode::SUCCESS.into(),
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
                    status: StatusCode::NO_PARTITION.into(),
                    ret_msg: "Partition not found".to_string(),
                    data_file_len: 0
                })
            );
        }

        let app = app_option.unwrap();

        let data_index_wrapper = app.list_index(ReadingIndexViewContext {
            partition_id: PartitionedUId::from(app_id.to_string(), shuffle_id, partition_id)
        }).await;

        if data_index_wrapper.is_err() {
            let error_msg = data_index_wrapper.err();
            error!("Errors on getting localfile data index for app:[{}], error: {:?}", &app_id, error_msg);
            return Ok(
                Response::new(GetLocalShuffleIndexResponse {
                    index_data: Default::default(),
                    status: StatusCode::INTERNAL_ERROR.into(),
                    ret_msg: format!("{:?}", error_msg),
                    data_file_len: 0,
                })
            );
        }

        match data_index_wrapper.unwrap() {
            ResponseDataIndex::local(data_index) => {
                Ok(
                    Response::new(GetLocalShuffleIndexResponse {
                        index_data: data_index.index_data,
                        status: StatusCode::SUCCESS.into(),
                        ret_msg: "".to_string(),
                        data_file_len: data_index.data_file_len
                    })
                )
            },
            _ => Ok(
                Response::new(GetLocalShuffleIndexResponse {
                    index_data: Default::default(),
                    status: StatusCode::INTERNAL_ERROR.into(),
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

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(
                Response::new(GetLocalShuffleDataResponse {
                    data: Default::default(),
                    status: StatusCode::NO_REGISTER.into(),
                    ret_msg: "No such app in this shuffle server".to_string(),
                })
            );
        }

        let data_fetched_result = app.unwrap().select(ReadingViewContext {
            uid: PartitionedUId {
                app_id: app_id.to_string(),
                shuffle_id,
                partition_id
            },
            reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(
                req.offset, req.length as i64
            )
        }).await;

        if data_fetched_result.is_err() {
            let err_msg = data_fetched_result.err();
            error!("Errors on getting localfile index for app:[{}], error: {:?}", &app_id, err_msg);
            return Ok(Response::new(GetLocalShuffleDataResponse {
                data: Default::default(),
                status: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("{:?}", err_msg),
            }));
        }
        
        Ok(Response::new(GetLocalShuffleDataResponse {
            data: data_fetched_result.unwrap().from_local(),
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string()
        }))
    }

    async fn get_memory_shuffle_data(&self, request: Request<GetMemoryShuffleDataRequest>) -> Result<Response<GetMemoryShuffleDataResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id: i32 = req.shuffle_id;
        let partition_id = req.partition_id;

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(
                Response::new(GetMemoryShuffleDataResponse {
                    shuffle_data_block_segments: Default::default(),
                    data: Default::default(),
                    status: StatusCode::NO_REGISTER.into(),
                    ret_msg: "No such app in this shuffle server".to_string(),
                })
            );
        }

        let data_fetched_result = app.unwrap().select(ReadingViewContext {
            uid: PartitionedUId {
                app_id: app_id.to_string(),
                shuffle_id,
                partition_id
            },
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                req.last_block_id, req.read_buffer_size as i64
            )
        }).await;

        if data_fetched_result.is_err() {
            let error_msg = data_fetched_result.err();
            error!("Errors on getting data from memory for [{}], error: {:?}", &app_id, error_msg);
            return Ok(Response::new(GetMemoryShuffleDataResponse {
                shuffle_data_block_segments: vec![],
                data: Default::default(),
                status: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("{:?}", error_msg),
            }));
        }

        let data = data_fetched_result.unwrap().from_memory();

        Ok(Response::new(GetMemoryShuffleDataResponse {
            shuffle_data_block_segments: data.shuffle_data_block_segments.into_iter().map(|x| x.into()).collect(),
            data: data.data,
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string()
        }))
    }

    async fn commit_shuffle_task(&self, request: Request<ShuffleCommitRequest>) -> Result<Response<ShuffleCommitResponse>, Status> {
        warn!("It has not been supported of committing shuffle data");
        Ok(Response::new(ShuffleCommitResponse {
            commit_count: 0,
            status: StatusCode::INTERNAL_ERROR.into(),
            ret_msg: "Not supported".to_string(),
        }))
    }

    async fn report_shuffle_result(&self, request: Request<ReportShuffleResultRequest>) -> Result<Response<ReportShuffleResultResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;
        let partition_to_block_ids = req.partition_to_block_ids;

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(
                Response::new(ReportShuffleResultResponse {
                    status: StatusCode::NO_REGISTER.into(),
                    ret_msg: "No such app in this shuffle server".to_string(),
                })
            );
        }
        let app = app.unwrap();

        for partition_to_block_id in partition_to_block_ids {
            let partition_id = partition_to_block_id.partition_id;
            debug!("Reporting partition:{} {} blocks", partition_id, partition_to_block_id.block_ids.len());
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
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string()
        }))
    }

    async fn get_shuffle_result(&self, request: Request<GetShuffleResultRequest>) -> Result<Response<GetShuffleResultResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;
        let partition_id = req.partition_id;

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(
                Response::new(GetShuffleResultResponse {
                    status: StatusCode::NO_REGISTER.into(),
                    ret_msg: "No such app in this shuffle server".to_string(),
                    serialized_bitmap: Default::default(),
                })
            );
        }

        let data = app.unwrap().get_block_ids(GetBlocksContext {
            uid: PartitionedUId {
                app_id,
                shuffle_id,
                partition_id
            }
        }).await.unwrap();

        Ok(Response::new(GetShuffleResultResponse {
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string(),
            serialized_bitmap: data
        }))
    }

    async fn get_shuffle_result_for_multi_part(&self, request: Request<GetShuffleResultForMultiPartRequest>) -> Result<Response<GetShuffleResultForMultiPartResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(
                Response::new(GetShuffleResultForMultiPartResponse {
                    status: StatusCode::NO_REGISTER.into(),
                    ret_msg: "No such app in this shuffle server".to_string(),
                    serialized_bitmap: Default::default(),
                })
            );
        }
        let app = app.unwrap();

        let mut bytes_mut = BytesMut::new();
        for partition_id in req.partitions {
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
                status: StatusCode::SUCCESS.into(),
                ret_msg: "".to_string()
            })
        )
    }

    async fn require_buffer(&self, request: Request<RequireBufferRequest>) -> Result<Response<RequireBufferResponse>, Status> {
        let req = request.into_inner();
        let app_id = req.app_id;
        let shuffle_id = req.shuffle_id;

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(
                Response::new(RequireBufferResponse {
                    require_buffer_id: 0,
                    status: StatusCode::NO_REGISTER.into(),
                    ret_msg: "No such app in this shuffle server".to_string(),
                })
            );
        }
        let app = app.unwrap();

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
        let app_id = request.into_inner().app_id;
        info!("Accepted heartbeat for app: {:#?}", &app_id);

        let app = self.app_manager_ref.get_app(&app_id);
        if app.is_none() {
            return Ok(
                Response::new(AppHeartBeatResponse {
                    status: StatusCode::NO_REGISTER.into(),
                    ret_msg: "No such app in this shuffle server".to_string(),
                })
            );
        }

        let app = app.unwrap();
        let _ = app.heartbeat();

        Ok(Response::new(AppHeartBeatResponse {
            status: StatusCode::SUCCESS.into(),
            ret_msg: "".to_string()
        }))
    }
}