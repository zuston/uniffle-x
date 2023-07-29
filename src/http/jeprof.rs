use serde::{Deserialize, Serialize};
use std::time::Duration;
use log::error;

use tokio::time::sleep as delay_for;
use poem::{IntoResponse, Request, RouteMethod};
use poem::http::StatusCode;
use poem::error::ResponseError;
use tempfile::Builder;

use crate::mem_allocator::*;
use crate::mem_allocator::error::ProfError;
use crate::mem_allocator::error::ProfError::IoError;
use super::Handler;

#[derive(Deserialize, Serialize)]
#[serde(default)]
pub struct JeProfRequest {
    pub(crate) duration: u64,
    pub(crate) keep_profiling: bool,
}

impl Default for JeProfRequest {
    fn default() -> Self {
        JeProfRequest {
            duration: 15,
            keep_profiling: false,
        }
    }
}

// converts profiling error to http error
impl ResponseError for ProfError {
    fn status(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[poem::handler]
async fn jeprof_handler(req: &Request) -> poem::Result<impl IntoResponse> {
    let req = req.params::<JeProfRequest>()?;
    // when memory-prof feature is not enabled, return error
    if !is_prof_enabled() {
        return Err(ProfError::MemProfilingNotEnabled.into());
    }
    // todo: maybe we can use a global lock to prevent multiple profiling at the same time
    // enables heap profiling
    let _ = activate_prof().map_err(|e| {
        let msg = format!("could not start profiling: {:?}", e);
        error!("{}", msg);
        e
    })?;
    // create tmp_file
    let tmp_file = Builder::new()
        .prefix("heap_dump_")
        .suffix(".prof")
        .tempfile()
        .map_err(|e| IoError(e))?;
    let path = tmp_file.path();
    delay_for(Duration::from_secs(req.duration)).await;
    // dump heap profile
    let buf: Vec<u8> = dump_prof(&path.to_string_lossy()).map_err(|e| {
        let msg = format!("could not dump heap profile: {:?}", e);
        error!("{}", msg);
        e
    })?;
    let len = buf.len();
    if !req.keep_profiling {
        // disables heap profiling
        let _ = deactivate_prof().map_err(|e| {
            let msg = format!("could not stop profiling: {:?}", e);
            error!("{}", msg);
            e
        })?;
    }
    Ok(buf.with_header("content-length", len).with_header(
        "Content-Disposition",
        format!("attachment; filename=\"{}\"", &path.to_string_lossy()),
    ).into_response())
}

pub struct JeProfHandler { }

impl Default for JeProfHandler {
    fn default() -> Self {
        JeProfHandler { }
    }
}

impl Handler for JeProfHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(jeprof_handler)
    }

    fn get_route_path(&self) -> String {
        "/debug/heap/profile".to_string()
    }
}

#[cfg(test)]
mod test {
    use crate::http::jeprof::JeProfHandler;
    use crate::http::Handler;
    use poem::test::TestClient;
    use poem::Route;
    use tonic::codegen::http::StatusCode;

    #[tokio::test]
    async fn test_router() {
        let handler = JeProfHandler::default();
        let app = Route::new().at(handler.get_route_path(), handler.get_route_method());
        let cli = TestClient::new(app);
        let resp = cli
            .get("/debug/heap/profile")
            .query("seconds", &10)
            .send()
            .await;

        #[cfg(feature = "mem-profiling")]
        resp.assert_status_is_ok();
        #[cfg(not(feature = "mem-profiling"))]
        resp.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
    }
}