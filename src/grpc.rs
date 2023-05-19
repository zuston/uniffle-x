use tonic::{Request, Response, Status};
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
        todo!()
    }

    async fn unregister_shuffle(&self, request: Request<ShuffleUnregisterRequest>) -> Result<Response<ShuffleUnregisterResponse>, Status> {
        todo!()
    }

    async fn send_shuffle_data(&self, request: Request<SendShuffleDataRequest>) -> Result<Response<SendShuffleDataResponse>, Status> {
        let req = request.into_inner();
        let appId = req.app_id;
        let shuffleId: i32 = req.shuffle_id;
        todo!()
    }

    async fn get_local_shuffle_index(&self, request: Request<GetLocalShuffleIndexRequest>) -> Result<Response<GetLocalShuffleIndexResponse>, Status> {
        todo!()
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