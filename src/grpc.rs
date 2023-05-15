use tonic::{Request, Response, Status};
use crate::app::{MemoryReadOptions, PartitionedDataContext, ReadOptions};
use crate::AppManagerRef;
use crate::proto::uniffle::shuffle_server_server::ShuffleServer;
use crate::proto::uniffle::{AppHeartBeatRequest, AppHeartBeatResponse, FinishShuffleRequest, FinishShuffleResponse, GetLocalShuffleDataRequest, GetLocalShuffleDataResponse, GetLocalShuffleIndexRequest, GetLocalShuffleIndexResponse, GetMemoryShuffleDataRequest, GetMemoryShuffleDataResponse, GetShuffleResultForMultiPartRequest, GetShuffleResultForMultiPartResponse, GetShuffleResultRequest, GetShuffleResultResponse, ReportShuffleResultRequest, ReportShuffleResultResponse, RequireBufferRequest, RequireBufferResponse, SendShuffleDataRequest, SendShuffleDataResponse, ShuffleCommitRequest, ShuffleCommitResponse, ShuffleRegisterRequest, ShuffleRegisterResponse, ShuffleUnregisterRequest, ShuffleUnregisterResponse};

#[derive(Default)]
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
        let appId = request.into_inner().app_id;
        let status = match self.appManagerRef.register(appId) {
            Ok(_) => 0,
            _ => 1
        };
        let response = ShuffleRegisterResponse {
            status,
            ret_msg: "".to_string()
        };
        Ok(Response::new(response))
    }

    async fn unregister_shuffle(&self, request: Request<ShuffleUnregisterRequest>) -> Result<Response<ShuffleUnregisterResponse>, Status> {
        Ok(Response::new(ShuffleUnregisterResponse {
            status: 0,
            ret_msg: "".to_string()
        }))
    }

    async fn send_shuffle_data(&self, request: Request<SendShuffleDataRequest>) -> Result<Response<SendShuffleDataResponse>, Status> {
        let req = request.into_inner();
        let appId = req.app_id;
        let shuffleId: i32 = req.shuffle_id;

        let status =
            if let Some(app) = self.appManagerRef.get_app(appId.clone()) {
                for shuffleData in req.shuffle_data.into_iter() {
                    let partitionId = shuffleData.partition_id;
                    let ctx =
                        PartitionedDataContext::from(appId.clone(), shuffleId, partitionId);
                    app.put_data(ctx, shuffleData.into());
                }
                0
            } else {
                1
            };

        Ok(Response::new(SendShuffleDataResponse {
            status,
            ret_msg: "".to_string()
        }))
    }

    async fn get_local_shuffle_index(&self, request: Request<GetLocalShuffleIndexRequest>) -> Result<Response<GetLocalShuffleIndexResponse>, Status> {
        todo!()
    }

    async fn get_local_shuffle_data(&self, request: Request<GetLocalShuffleDataRequest>) -> Result<Response<GetLocalShuffleDataResponse>, Status> {
        todo!()
    }

    async fn get_memory_shuffle_data(&self, request: Request<GetMemoryShuffleDataRequest>) -> Result<Response<GetMemoryShuffleDataResponse>, Status> {
        let req = request.into_inner();
        let appId = req.app_id;

        let ctx =
            PartitionedDataContext::from(appId.clone(), req.shuffle_id, req.partition_id);
        
        let response = GetMemoryShuffleDataResponse {
            shuffle_data_block_segments: vec![],
            data: vec![],
            status: 0,
            ret_msg: "".to_string()
        };

        match self.appManagerRef.get_app(appId.clone()) {
            Some(app) => {
                let result = app.get_data(
                    ctx,
                    ReadOptions::Memory(MemoryReadOptions::from(req.read_buffer_size)),
                req.last_block_id);
                ()
            }
            _ => todo!()
        }

        Ok(Response::new(response))
    }

    async fn commit_shuffle_task(&self, request: Request<ShuffleCommitRequest>) -> Result<Response<ShuffleCommitResponse>, Status> {
        todo!()
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