use log::error;
use std::num::NonZeroI32;
use std::time::Duration;
use serde::{Deserialize,Serialize};
use tokio::time::sleep as delay_for;
use pprof::ProfilerGuard;
use pprof::protos::Message;
use warp::{Rejection, Reply};

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

pub async fn pprof_handler(req: PProfRequest) -> Result<impl Reply, Rejection> {
    let mut body: Vec<u8> = Vec::new();

    let guard = ProfilerGuard::new(req.frequency.into()).map_err(|e| {
        error!("could not start profiling: {:?}", e);
        warp::reject::reject()
    })?;
    delay_for(Duration::from_secs(req.seconds)).await;
    let report = guard.report().build().map_err(|e| {
        error!("could not build profiling report: {:?}", e);
        warp::reject::reject()
    })?;
    let profile = report.pprof()
        .map_err(|e| {
            error!("could not get pprof profile: {:?}", e);
            warp::reject::reject()
        })?;
    profile.write_to_vec(&mut body).map_err(|e| {
        error!("could not write pprof profile: {:?}", e);
        warp::reject::reject()
    })?;
    Ok(body)
}
