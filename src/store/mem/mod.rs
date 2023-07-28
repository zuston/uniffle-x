pub struct MemoryBufferTicket {
    id: i64,
    created_time: u64,
    size: i64,
}

impl MemoryBufferTicket {
    pub fn new(id: i64, created_time: u64, size: i64) -> Self {
        Self {
            id,
            created_time,
            size,
        }
    }

    pub fn get_size(&self) -> i64 {
        self.size
    }

    pub fn is_timeout(&self, timeout_sec: i64) -> bool {
        crate::util::current_timestamp_sec() - self.created_time > timeout_sec as u64
    }

    pub fn get_id(&self) -> i64 {
        self.id
    }
}
