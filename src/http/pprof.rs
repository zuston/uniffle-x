use log::error;
use std::num::NonZeroI32;
use std::time::Duration;
use poem::{get, RouteMethod};
use poem::endpoint::{make, make_sync};
use serde::{Deserialize,Serialize};
use tokio::time::sleep as delay_for;
use pprof::ProfilerGuard;
use pprof::protos::Message;
use tracing_subscriber::fmt::format;
use crate::error::DatanodeError;
use crate::http::Handler;

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

async fn pprof() -> poem::Result<Vec<u8>, DatanodeError> {
    let req = PProfRequest::default();
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
    let profile = report.pprof()
        .map_err(|e| {
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
        get(make(|_| pprof()))
    }

    fn get_route_path(&self) -> String {
        "/debug/pprof/profile".to_string()
    }
}
