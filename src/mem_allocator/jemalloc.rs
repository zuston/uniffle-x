pub type Allocator = tikv_jemallocator::Jemalloc;
pub const fn allocator() -> Allocator {
    tikv_jemallocator::Jemalloc
}