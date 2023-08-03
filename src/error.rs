use anyhow::Error;

use log::error;
use poem::error::ParseQueryError;
use thiserror::Error;
use tokio::sync::AcquireError;

#[derive(Error, Debug)]
#[allow(non_camel_case_types)]
pub enum WorkerError {
    #[error("There is no available disks in local file store")]
    NO_AVAILABLE_LOCAL_DISK,

    #[error("Internal error, it should not happen")]
    INTERNAL_ERROR,

    #[error("Partial data has been lost, corrupted path: {0}")]
    PARTIAL_DATA_LOST(String),

    #[error("Local disk:[{0}] owned by current partition has been corrupted")]
    LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(String),

    #[error("No enough memory to be allocated.")]
    NO_ENOUGH_MEMORY_TO_BE_ALLOCATED,

    #[error("The memory usage is limited by huge partition mechanism")]
    MEMORY_USAGE_LIMITED_BY_HUGE_PARTITION,

    #[error(transparent)]
    Other(#[from] anyhow::Error),

    #[error("Http request failed. {0}")]
    HTTP_SERVICE_ERROR(String),
}

impl From<AcquireError> for WorkerError {
    fn from(error: AcquireError) -> Self {
        WorkerError::Other(Error::new(error))
    }
}

impl From<ParseQueryError> for WorkerError {
    fn from(error: ParseQueryError) -> Self {
        WorkerError::Other(Error::new(error))
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;

    #[test]
    pub fn error_test() -> Result<()> {
        // bail macro means it will return directly.
        // bail!(WorkerError::APP_PURGE_EVENT_SEND_ERROR("error_test_app_id".into(), None));
        Ok(())
    }
}
