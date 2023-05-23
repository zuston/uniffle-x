use std::cell::Ref;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::os::macos::raw::stat;
use std::sync::{Arc};
use std::sync::atomic::{AtomicI64, Ordering};
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use crate::*;
use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingViewContext, RequireBufferContext, WritingViewContext};
use crate::app::ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE;
use crate::store::{DataSegment, PartitionedDataBlock, PartitionedMemoryData, ResponseData, ResponseDataIndex, Store};
use async_trait::async_trait;
use tokio::sync::Mutex;
use crate::config::MemoryStoreConfig;

#[derive(Clone)]
pub struct MemoryStore {
    // todo: change to RW lock
    state: DashMap<String, DashMap<i32, DashMap<i32, Arc<Mutex<StagingBuffer>>>>>,
    pub budget: MemoryBudget,
    memory_allocated_of_app: DashMap<String, i64>,
    pub memory_capacity: i64,
}

unsafe impl Send for MemoryStore {}
unsafe impl Sync for MemoryStore {}

impl MemoryStore {
    pub fn new(max_memory_size: i64) -> Self {
        MemoryStore {
            state: DashMap::new(),
            budget: MemoryBudget::new(max_memory_size),
            memory_allocated_of_app: DashMap::new(),
            memory_capacity: max_memory_size
        }
    }

    pub fn from(conf: MemoryStoreConfig) -> Self {
        MemoryStore {
            state: DashMap::new(),
            budget: MemoryBudget::new(conf.capacity),
            memory_allocated_of_app: DashMap::new(),
            memory_capacity: conf.capacity
        }
    }

    // todo: make this used size as a var
    pub async fn memory_usage_ratio(&self) -> f32 {
        let (capacity, allocated, used) = self.budget.snapshot().await;

        let mut in_flight = 0i64;
        for app_entry in self.state.iter() {
            for shuffle_entry in app_entry.value().iter() {
                for partition_entry in shuffle_entry.value().iter() {
                    for (_, blocks) in partition_entry.value().lock().await.in_flight.iter() {
                        for block in blocks {
                            in_flight += block.length as i64;
                        }
                    }
                }
            }
        }

        (allocated + used - in_flight) as f32 / capacity as f32
    }

    pub async fn free_memory(&self, size: i64) -> Result<bool> {
        self.budget.free_used(size).await
    }

    pub async fn get_required_spill_buffer(&self, target_len: i64) -> HashMap<PartitionedUId, Arc<Mutex<StagingBuffer>>> {
        // sort
        // get the spill buffers

        let mut sorted_tree_map = BTreeMap::new();
        let app_iter = self.state.iter();
        for app_entry in app_iter {
            for shuffle_entry in app_entry.value().iter() {
                for partition_entry in shuffle_entry.value().iter() {
                    let mut staging_size = 0;
                    for block in &partition_entry.value().lock().await.blocks {
                        staging_size += block.length;
                    }

                    let mut valset = sorted_tree_map.entry(staging_size).or_insert_with(||vec![]);
                    valset.push((app_entry.key().clone(), shuffle_entry.key().clone(), partition_entry.key().clone()));
                }
            }
        }

        let removed_size = self.memory_capacity - target_len;
        let current_remove = 0;

        let mut required_spill_buffers = HashMap::new();

        let iter = sorted_tree_map.iter().rev();
        'outer: for (size, vals) in iter {
            for (app_id, shuffle_id, partition_id) in vals {
                if current_remove >= removed_size {
                    break 'outer;
                }
                required_spill_buffers.insert(
                    PartitionedUId::from(app_id.to_string(), shuffle_id.clone(), partition_id.clone()),
                    self.get_underlying_partition_buffer(app_id, shuffle_id, partition_id)
                );
            }
        }

        required_spill_buffers
    }

    fn get_underlying_partition_buffer(&self, app_id: &String, shuffle_id: &i32, partition_id: &i32) -> Arc<Mutex<StagingBuffer>> {
        let app_entry = self.state.get(app_id).unwrap();
        let shuffle_entry = app_entry.get(shuffle_id).unwrap();
        let partition_entry = shuffle_entry.get(partition_id).unwrap();

        let buffer_cloned = partition_entry.value().clone();
        buffer_cloned
    }

    pub async fn release_in_flight_blocks_in_underlying_staging_buffer(&self, uid: PartitionedUId, in_flight_blocks_id: i64) -> Result<()> {
        let buffer = self.get_or_create_underlying_staging_buffer(uid);
        let mut buffer_ref = buffer.lock().await;
        buffer_ref.in_flight.remove(&in_flight_blocks_id);
        Ok(())
    }

    pub fn get_or_create_underlying_staging_buffer(&self, uid: PartitionedUId) -> Arc<Mutex<StagingBuffer>> {
        let app_id = uid.app_id;
        let shuffle_id = uid.shuffle_id;
        let partition_id = uid.partition_id;
        let app_level_entry = self.state.entry(app_id).or_insert_with(|| DashMap::new());
        let shuffle_level_entry = app_level_entry.entry(shuffle_id).or_insert_with(|| DashMap::new());
        let buffer = shuffle_level_entry.entry(partition_id).or_insert_with(|| Arc::new(Mutex::new(StagingBuffer::new())));
        buffer.clone()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn insert(&self, ctx: WritingViewContext) -> Result<()> {
        let uid = ctx.uid;
        let buffer = self.get_or_create_underlying_staging_buffer(uid.clone());
        let mut buffer_guarded = buffer.lock().await;

        let blocks = ctx.data_blocks;
        let mut added_size = 0i64;
        for block in blocks {
            added_size += block.length as i64;
            buffer_guarded.blocks.push(block);
        }

        let _ = self.budget.allocated_to_used(added_size).await;
        buffer_guarded.size += added_size;

        Ok(())
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData> {
        let uid = ctx.uid;
        let buffer = self.get_or_create_underlying_staging_buffer(uid);
        let mut buffer = buffer.lock().await;

        let options = ctx.reading_options;
        let (fetched_blocks, length) = match options {
            MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(last_block_id, max_size) => {
                // if the first time, it should read from blocks rather than in flight
                let mut fetched = vec![];
                let mut fetched_size = 0;
                if last_block_id != -1 {
                    let mut existence = false;
                    // todo: use the flatmap to simplify the logic
                    for (_, blocks) in buffer.in_flight.iter() {
                        for in_flight_block in blocks {
                            if !existence && last_block_id == in_flight_block.block_id {
                                existence = true;
                            }
                            if fetched_size > max_size {
                                break;
                            }
                            fetched_size += in_flight_block.length as i64;
                            fetched.push(in_flight_block);
                        }
                    }
                }

                for block in &buffer.blocks {
                    if fetched_size > max_size {
                        break;
                    }
                    fetched_size += block.length as i64;
                    fetched.push(block);
                }

                (fetched, fetched_size)
            }
            _ => (vec![], 0)
        };

        let mut bytes_holder = BytesMut::with_capacity(length as usize);
        let mut segments = vec![];
        let mut offset = 0;
        for block in fetched_blocks {
            let data = &block.data;
            bytes_holder.extend_from_slice(data);
            segments.push(DataSegment {
                block_id: block.block_id,
                offset,
                length: block.length,
                uncompress_length: block.uncompress_length,
                crc: block.crc,
                task_attempt_id: block.task_attempt_id
            });
            offset += block.length as i64;
        }

        Ok(ResponseData::mem(
            PartitionedMemoryData {
                shuffle_data_block_segments: segments,
                data: bytes_holder.freeze()
            }
        ))
    }

    async fn get_index(&mut self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex> {
        panic!("It should not be invoked.")
    }

    async fn require_buffer(&mut self, ctx: RequireBufferContext) -> Result<(bool, i64)> {
        let result = self.budget.pre_allocate(ctx.size).await;
        if result.is_ok() {
            let mut val = self.memory_allocated_of_app.entry(ctx.uid.app_id).or_insert_with(||0);
            *val += ctx.size;
        }
        result
    }

    async fn purge(&mut self, app_id: String) -> Result<()> {
        match self.memory_allocated_of_app.get(&app_id) {
            Some(val) => {
                self.budget.free_allocated(*val).await.unwrap();
            }
            _ => todo!()
        }

        self.state.remove(&app_id);
        Ok(())
    }
}


/// thread safe, this will be guarded by the lock
#[derive(Debug, Clone)]
pub struct StagingBuffer {
    pub size: i64,
    pub blocks: Vec<PartitionedDataBlock>,
    pub in_flight: BTreeMap<i64, Vec<PartitionedDataBlock>>,
    id_generator: i64
}

impl StagingBuffer {
    pub fn new() -> StagingBuffer {
        StagingBuffer {
            size: 0,
            blocks: vec![],
            in_flight: BTreeMap::new(),
            id_generator: 0
        }
    }

    pub fn add_blocks_to_send(&mut self, blocks: Vec<PartitionedDataBlock>) -> Result<i64> {
        let id = self.id_generator;
        self.id_generator += 1;
        self.in_flight.insert(id.clone(), blocks);
        Ok(id)
    }
}

#[derive(Clone)]
pub struct MemoryBudget {
    inner: Arc<Mutex<MemoryBudgetInner>>
}

struct MemoryBudgetInner {
    capacity: i64,
    allocated: i64,
    used: i64,
    allocation_incr_id: i64
}

impl MemoryBudget {
    fn new(capacity: i64) -> MemoryBudget {
        MemoryBudget {
            inner: Arc::new(
                Mutex::new(
                    MemoryBudgetInner {
                        capacity,
                        allocated: 0,
                        used: 0,
                        allocation_incr_id: 0
                    }
                )
            )
        }
    }

    pub async fn snapshot(&self) -> (i64, i64, i64) {
        let mut inner = self.inner.lock().await;
        (
            inner.capacity,
            inner.allocated,
            inner.used
        )
    }


    async fn pre_allocate(&mut self, size: i64) -> Result<(bool, i64)> {
        let mut inner = self.inner.lock().await;
        let free_space = inner.capacity - inner.allocated - inner.used;
        if free_space < size {
            Ok((false, -1))
        } else {
            inner.allocated += size;
            let now = inner.allocation_incr_id;
            inner.allocation_incr_id += 1;
            Ok((true, now))
        }
    }

    async fn allocated_to_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().await;
        if inner.allocated < size {
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        inner.used += size;
        Ok(true)
    }

    async fn free_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().await;
        if inner.used < size {
            inner.used = 0;
            // todo: metric
        } else {
            inner.used -= size;
        }
        Ok(true)
    }

    async fn free_allocated(&mut self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().await;
        if inner.allocated < size {
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        Ok(true)
    }
}


#[cfg(test)]
mod test {
    use core::panic;
    use std::borrow::Borrow;
    use std::collections::BTreeMap;
    use std::io::Read;
    use async_trait::async_trait;
    use bytes::{Bytes, BytesMut};
    use dashmap::DashMap;
    use log::Level::Debug;
    use crate::app::{PartitionedUId, ReadingOptions, ReadingViewContext, RequireBufferContext, WritingViewContext};
    use crate::store::{PartitionedDataBlock, ResponseData, Store};
    use crate::store::memory::MemoryStore;
    use crate::store::ResponseDataIndex::local;

    #[tokio::test]
    async fn test_read_from_memory_when_data_in_flight() {

    }

    #[tokio::test]
    async fn test_allocated_and_purge_for_memory() {
        let mut store = MemoryStore::new(1024 * 1024 * 1024);
        let ctx = RequireBufferContext {
            uid: PartitionedUId {
                app_id: "100".to_string(),
                shuffle_id: 0,
                partition_id: 0
            },
            size: 10000
        };
        match store.require_buffer(ctx).await {
            Ok((_, _)) => {
                store.purge("100".to_string()).await;
            }
            _ => panic!()
        }

        let budget = store.budget.inner.lock().await;
        assert_eq!(0, budget.allocated);
        assert_eq!(0, budget.used);
        assert_eq!(1024 * 1024 * 1024, budget.capacity);

        assert_eq!(false, store.state.contains_key(&"100".to_string()));
    }

    #[tokio::test]
    async fn test_put_and_get_for_memory() {
        let mut store = MemoryStore::new(1024 * 1024 * 1024);
        let writingCtx = WritingViewContext {
            uid: Default::default(),
            data_blocks: vec![
                PartitionedDataBlock {
                    block_id: 0,
                    length: 10,
                    uncompress_length: 100,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 0
                },
                PartitionedDataBlock {
                    block_id: 1,
                    length: 20,
                    uncompress_length: 200,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 1
                }
            ]
        };
        store.insert(writingCtx).await.unwrap();

        let readingCtx = ReadingViewContext {
            uid: Default::default(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000)
        };

        match store.get(readingCtx).await.unwrap() {
            ResponseData::mem(data) => {
                assert_eq!(data.shuffle_data_block_segments.len(), 2);
                assert_eq!(data.shuffle_data_block_segments.get(0).unwrap().offset, 0);
                assert_eq!(data.shuffle_data_block_segments.get(1).unwrap().offset, 10);
            },
            _ => panic!("should not")
        }
    }

    #[tokio::test]
    async fn test_async_trait() {
        use anyhow::{Result, anyhow};
        #[async_trait]
        trait Person {
            async fn get(&self) -> anyhow::Result<()>;
            async fn put(&self);
        }

        struct Man {
            age: i32
        }

        #[async_trait]
        impl Person for Man {
            async fn get(&self) -> anyhow::Result<()>{
                Ok(())
            }

            async fn put(&self) {

            }
        }

        let man = Man {
            age: 10
        };

        man.get().await;
        man.put().await;
    }

    #[test]
    fn test_ratio() {
        let a = 10i32;
        let b = 20i32;
        let c = a as f32 / b as f32;
        println!("{}", c)
    }

    #[test]
    fn test_multi_ref() {
        // let mapper = DashMap::new();
        // mapper.insert("1".to_string(), "hello".to_string());
        // mapper.insert("2".to_string(), "world".to_string());
        //
        // let mut sorted = BTreeMap::new();
        //
        // let iter1 = mapper.iter();
        // for entry in iter1 {
        //     let value = entry.as_str();
        //     sorted.insert(1, value);
        // }
    }
}
