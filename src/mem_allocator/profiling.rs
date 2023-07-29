use std::ffi::{c_char, CString};
use std::fs::File;
use std::io::Read;

use super::error::{ProfError, ProfResult};

// C string should end with a '\0'.
const OPT_PROF: &[u8] = b"opt.prof\0";
const PROF_ACTIVE: &[u8] = b"prof.active\0";
const PROF_DUMP: &[u8] = b"prof.dump\0";

pub fn is_prof_enabled() -> bool {
    unsafe { tikv_jemalloc_ctl::raw::read::<bool>(OPT_PROF).unwrap_or(false) }
}

pub fn activate_prof() -> ProfResult<()> {
    unsafe {
        if let Err(e) = tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, true) {
            return Err(ProfError::JemallocError(format!(
                "failed to activate profiling: {}",
                e
            )));
        }
    }
    Ok(())
}

pub fn deactivate_prof() -> ProfResult<()> {
    unsafe {
        if let Err(e) = tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, false) {
            return Err(ProfError::JemallocError(format!(
                "failed to deactivate profiling: {}",
                e
            )));
        }
    }
    Ok(())
}

/// Dump the profile to the `path`. path usually is a temp file generated by `tempfile` crate.
pub fn dump_prof(path: &str) -> ProfResult<Vec<u8>> {
    let mut bytes = CString::new(path)?.into_bytes_with_nul();
    let ptr = bytes.as_mut_ptr() as *mut c_char;
    unsafe {
        if let Err(e) = tikv_jemalloc_ctl::raw::write(PROF_DUMP, ptr) {
            return Err(ProfError::JemallocError(format!(
                "failed to dump the profile to {:?}: {}",
                path, e
            )));
        }
    }
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
}
