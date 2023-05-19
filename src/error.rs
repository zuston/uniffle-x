use std::fmt::{Display, Formatter, Write};
use crossbeam_channel::SendError;
use log::error;
use thiserror::Error;
use crate::app::PurgeEvent;

#[derive(Error, Debug)]
pub enum DatanodeError {
    #[error("Failed to send purge event to app manager channel")]
    PURGE_EVENT_SEND_ERROR(#[from] SendError<PurgeEvent>),
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
