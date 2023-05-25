use log::error;
use toml::Value::String;
use tonic::{Request, Response, Status};
use crate::app::{PartitionedUId, ReadingIndexViewContext, WritingViewContext};
use crate::AppManagerRef;
use crate::proto::uniffle::shuffle_server_server::ShuffleServer;
use crate::proto::uniffle::{AppHeartBeatRequest, AppHeartBeatResponse, FinishShuffleRequest, FinishShuffleResponse, GetLocalShuffleDataRequest, GetLocalShuffleDataResponse, GetLocalShuffleIndexRequest, GetLocalShuffleIndexResponse, GetMemoryShuffleDataRequest, GetMemoryShuffleDataResponse, GetShuffleResultForMultiPartRequest, GetShuffleResultForMultiPartResponse, GetShuffleResultRequest, GetShuffleResultResponse, ReportShuffleResultRequest, ReportShuffleResultResponse, RequireBufferRequest, RequireBufferResponse, SendShuffleDataRequest, SendShuffleDataResponse, ShuffleCommitRequest, ShuffleCommitResponse, ShuffleRegisterRequest, ShuffleRegisterResponse, ShuffleUnregisterRequest, ShuffleUnregisterResponse};
use crate::proto::uniffle::coordinator_server_server::CoordinatorServer;
use crate::store::{PartitionedData, PartitionedDataBlock, ResponseDataIndex};

pub struct DefaultShuffleServer {
    appManagerRef: AppManagerRef
}

impl DefaultShuffleServer {
    pub fn from(appManagerRef: AppManagerRef) -> DefaultShuffleServer {
        DefaultShuffleServer {
            appManagerRef
        }
    }
}

#[tonic::async_trait]
impl ShuffleServer for DefaultShuffleServer {
    async fn register_shuffle(&self, request: Request<ShuffleRegisterRequest>) -> Result<Response<ShuffleRegisterResponse>, Status> {
        let inner = request.into_inner();
        let status = match self.appManagerRef.register(inner.app_id, inner.shuffle_id) {
            Ok(_) => 0,
            _ => 1
        };
        Ok(
            Response::new(ShuffleRegisterResponse {
                status,
                ret_msg: "".to_string()
            })
        )
    }

    async fn unregister_shuffle(&self, request: Request<ShuffleUnregisterRequest>) -> Result<Response<ShuffleUnregisterResponse>, Status> {
        // todo
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

        let app_option = self.appManagerRef.get_app(&app_id);

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

        let app_option = self.appManagerRef.get_app(&app_id);

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
        todo!()
    }

    async fn get_memory_shuffle_data(&self, request: Request<GetMemoryShuffleDataRequest>) -> Result<Response<GetMemoryShuffleDataResponse>, Status> {
        todo!()
    }

    async fn commit_shuffle_task(&self, request: Request<ShuffleCommitRequest>) -> Result<Response<ShuffleCommitResponse>, Status> {
        panic!("It has not been supported.")
    }

    async fn report_shuffle_result(&self, request: Request<ReportShuffleResultRequest>) -> Result<Response<ReportShuffleResultResponse>, Status> {
        todo!()
    }

    async fn get_shuffle_result(&self, request: Request<GetShuffleResultRequest>) -> Result<Response<GetShuffleResultResponse>, Status> {
        todo!()
    }

    async fn get_shuffle_result_for_multi_part(&self, request: Request<GetShuffleResultForMultiPartRequest>) -> Result<Response<GetShuffleResultForMultiPartResponse>, Status> {
        todo!()
    }

    async fn finish_shuffle(&self, request: Request<FinishShuffleRequest>) -> Result<Response<FinishShuffleResponse>, Status> {
        todo!()
    }

    async fn require_buffer(&self, request: Request<RequireBufferRequest>) -> Result<Response<RequireBufferResponse>, Status> {
        todo!()
    }

    async fn app_heartbeat(&self, request: Request<AppHeartBeatRequest>) -> Result<Response<AppHeartBeatResponse>, Status> {
        todo!()
    }
}