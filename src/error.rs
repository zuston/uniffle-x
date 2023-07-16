use std::fmt::{Display, Formatter, Write};
use anyhow::Error;
use crossbeam_channel::SendError;
use log::error;
use thiserror::Error;
use tokio::sync::AcquireError;
use crate::app::PurgeEvent;
use crate::error::DatanodeError::Other;

#[derive(Error, Debug)]
pub enum DatanodeError {
    #[error("There is no available disks in local file store")]
    NO_AVAILABLE_LOCAL_DISK,

    #[error("Internal error, it should not happen")]
    INTERNAL_ERROR,

    #[error("Partial data has been lost, corrupted path: {0}")]
    PARTIAL_DATA_LOST(String),

    #[error("Local disk:[{0}] owned by current partition has been corrupted")]
    LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<AcquireError> for DatanodeError {
    fn from(error: AcquireError) -> Self {
        Other(Error::new(error))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Result, anyhow, bail};
    use crate::error::DatanodeError;

    #[test]
    pub fn error_test() -> Result<()>{
        // bail macro means it will return directly.
        // bail!(DatanodeError::APP_PURGE_EVENT_SEND_ERROR("error_test_app_id".into(), None));
        Ok(())
    }
}
