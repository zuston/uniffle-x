// Allocators
#[cfg(all(unix, feature = "jemalloc"))]
#[path = "jemalloc.rs"]
mod imp;

#[cfg(not(all(unix, feature = "jemalloc")))]
#[path = "system_std.rs"]
mod imp;

// set default allocator
#[global_allocator]
static ALLOC: imp::Allocator = imp::allocator();

pub mod error;
pub type AllocStats = Vec<(&'static str, usize)>;

// when memory-prof feature is enabled, provide empty profiling functions
#[cfg(not(all(unix, feature = "memory-prof")))]
mod default;
#[cfg(not(all(unix, feature = "memory-prof")))]
pub use default::*;

// when memory-prof feature is enabled, provide jemalloc profiling functions
#[cfg(all(unix, feature = "memory-prof"))]
mod profiling;
#[cfg(all(unix, feature = "memory-prof"))]
pub use profiling::*;
