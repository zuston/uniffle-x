use crate::error::DatanodeError;
use crate::http::Handler;
use log::error;

use poem::{handler, Request, RouteMethod};
use pprof::protos::Message;
use pprof::ProfilerGuard;
use serde::{Deserialize, Serialize};
use std::num::NonZeroI32;
use std::time::Duration;
use tokio::time::sleep as delay_for;

#[derive(Deserialize, Serialize)]
#[serde(default)]
pub struct PProfRequest {
    pub(crate) seconds: u64,
    pub(crate) frequency: NonZeroI32,
}

impl Default for PProfRequest {
    fn default() -> Self {
        PProfRequest {
            seconds: 5,
            frequency: NonZeroI32::new(100).unwrap(),
        }
    }
}

#[handler]
async fn pprof_handler(req: &Request) -> poem::Result<Vec<u8>, DatanodeError> {
    let req = req.params::<PProfRequest>()?;
    let mut body: Vec<u8> = Vec::new();

    let guard = ProfilerGuard::new(req.frequency.into()).map_err(|e| {
        let msg = format!("could not start profiling: {:?}", e);
        error!("{}", msg);
        DatanodeError::HTTP_SERVICE_ERROR(msg)
    })?;
    delay_for(Duration::from_secs(req.seconds)).await;
    let report = guard.report().build().map_err(|e| {
        let msg = format!("could not build profiling report: {:?}", e);
        error!("{}", msg);
        DatanodeError::HTTP_SERVICE_ERROR(msg)
    })?;
    let profile = report.pprof().map_err(|e| {
        let msg = format!("could not get pprof profile: {:?}", e);
        error!("{}", msg);
        DatanodeError::HTTP_SERVICE_ERROR(msg)
    })?;
    profile.write_to_vec(&mut body).map_err(|e| {
        let msg = format!("could not write pprof profile: {:?}", e);
        error!("{}", msg);
        DatanodeError::HTTP_SERVICE_ERROR(msg)
    })?;
    Ok(body)
}

pub struct PProfHandler {}

impl Default for PProfHandler {
    fn default() -> Self {
        Self {}
    }
}

impl Handler for PProfHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(pprof_handler)
    }

    fn get_route_path(&self) -> String {
        "/debug/pprof/profile".to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::http::pprof::PProfHandler;
    use crate::http::Handler;
    use poem::test::TestClient;
    use poem::Route;

    #[tokio::test]
    async fn test_router() {
        let handler = PProfHandler::default();
        let app = Route::new().at(handler.get_route_path(), handler.get_route_method());
        let cli = TestClient::new(app);
        let resp = cli
            .get("/debug/pprof/profile")
            .query("seconds", &4)
            .query("frequency", &100)
            .send()
            .await;
        resp.assert_status_is_ok();
    }
}
