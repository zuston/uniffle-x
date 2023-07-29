use super::error::{ProfError, ProfResult};

pub fn is_prof_enabled() -> bool {
    false
}

pub fn dump_prof(_path: &str) -> ProfResult<Vec<u8>> {
    Err(ProfError::MemProfilingNotEnabled)
}

pub fn activate_prof() -> ProfResult<()> {
    Err(ProfError::MemProfilingNotEnabled)
}

pub fn deactivate_prof() -> ProfResult<()> {
    Err(ProfError::MemProfilingNotEnabled)
}