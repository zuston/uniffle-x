use std::cell::Ref;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::slice::Iter;
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
                    for block in &partition_entry.value().lock().await.staging {
                        staging_size += block.length;
                    }

                    let valset = sorted_tree_map.entry(staging_size).or_insert_with(||vec![]);
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

    pub(crate) fn read_partial_data_with_max_size_limit<'a>(&'a self, blocks: Vec<&'a PartitionedDataBlock>, fetched_size_limit: i64) -> (Vec<&PartitionedDataBlock>, i64) {
        let mut fetched = vec![];
        let mut fetched_size = 0;

        for block in blocks {
            if fetched_size >= fetched_size_limit {
                break;
            }
            fetched_size += block.length as i64;
            fetched.push(block);
        }

        (fetched, fetched_size)
    }
}

#[async_trait]
impl Store for MemoryStore {
    fn start(self: Arc<Self>) {
        todo!()
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<()> {
        let uid = ctx.uid;
        let buffer = self.get_or_create_underlying_staging_buffer(uid.clone());
        let mut buffer_guarded = buffer.lock().await;

        let blocks = ctx.data_blocks;
        let mut added_size = 0i64;
        for block in blocks {
            added_size += block.length as i64;
            buffer_guarded.staging.push(block);
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
                let mut last_block_id = last_block_id.clone();
                let mut in_flight_flatten_blocks = vec![];
                for (_, blocks) in buffer.in_flight.iter() {
                    for in_flight_block in blocks {
                        in_flight_flatten_blocks.push(in_flight_block);
                    }
                }
                let in_flight_flatten_blocks = Arc::new(in_flight_flatten_blocks);

                let mut staging_blocks = vec![];
                for block in &buffer.staging {
                    staging_blocks.push(block);
                }
                let staging_blocks = Arc::new(staging_blocks);

                let mut candidate_blocks = vec![];
                // todo: optimize to the way of recursion
                let mut exited = false;
                while !exited {
                    exited = true;
                    if last_block_id == -1 {
                        // Anyway, it will always read from in_flight to staging
                        let mut extends: Vec<&PartitionedDataBlock> = vec![];
                        extends.extend_from_slice(&*in_flight_flatten_blocks);
                        extends.extend_from_slice(&*staging_blocks);
                        candidate_blocks = extends;
                    } else {
                        // check whether the in_fight_blocks exist the last_block_id
                        let mut in_flight_exist = false;
                        let mut unread_in_flight_blocks = vec![];
                        for block in in_flight_flatten_blocks.clone().iter() {
                            if !in_flight_exist {
                                if block.block_id == last_block_id {
                                    in_flight_exist = true;
                                }
                                continue;
                            }
                            unread_in_flight_blocks.push(*block);
                        }

                        if in_flight_exist {
                            let mut extends: Vec<&PartitionedDataBlock> = vec![];
                            extends.extend_from_slice(&*unread_in_flight_blocks);
                            extends.extend_from_slice(&*staging_blocks);
                            candidate_blocks = extends;
                        } else {
                            let mut staging_exist = false;
                            let mut unread_staging_buffers = vec![];
                            for block in staging_blocks.clone().iter() {
                                if !staging_exist {
                                    if block.block_id == last_block_id {
                                        staging_exist = true;
                                    }
                                    continue;
                                }
                                unread_staging_buffers.push(*block);
                            }

                            if staging_exist {
                                candidate_blocks = unread_staging_buffers;
                            } else {
                                // when the last_block_id is not found, maybe the partial has been flush
                                // to file. if having rest data, let's read it from the head
                                exited = false;
                                last_block_id = -1;
                            }
                        }
                    }
                }

                self.read_partial_data_with_max_size_limit(candidate_blocks, max_size)
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

    async fn get_index(&self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex> {
        panic!("It should not be invoked.")
    }

    async fn require_buffer(&self, ctx: RequireBufferContext) -> Result<(bool, i64)> {
        let result = self.budget.pre_allocate(ctx.size).await;
        if result.is_ok() {
            let mut val = self.memory_allocated_of_app.entry(ctx.uid.app_id).or_insert_with(||0);
            *val += ctx.size;
        }
        result
    }

    async fn purge(&self, app_id: String) -> Result<()> {
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
    pub staging: Vec<PartitionedDataBlock>,
    pub in_flight: BTreeMap<i64, Vec<PartitionedDataBlock>>,
    id_generator: i64
}

impl StagingBuffer {
    pub fn new() -> StagingBuffer {
        StagingBuffer {
            size: 0,
            staging: vec![],
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


    async fn pre_allocate(&self, size: i64) -> Result<(bool, i64)> {
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

    async fn free_allocated(&self, size: i64) -> Result<bool> {
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
    use crate::app::{PartitionedUId, ReadingOptions, ReadingViewContext, RequireBufferContext, WritingViewContext};
    use crate::config::HybridStoreConfig;
    use crate::store::{PartitionedDataBlock, PartitionedMemoryData, ResponseData, Store};
    use crate::store::memory::MemoryStore;
    use crate::store::ResponseData::mem;
    use crate::store::ResponseDataIndex::local;

    #[tokio::test]
    async fn test_read_buffer_in_flight() {
        let mut store = MemoryStore::new(1024);
        let uid = PartitionedUId {
            app_id: "100".to_string(),
            shuffle_id: 0,
            partition_id: 0
        };
        let writingViewCtx = create_writing_ctx_with_blocks(10, 10, uid.clone());
        let _ = store.insert(writingViewCtx).await;


        let default_single_read_size = 20;

        // case1: read from -1
        let mem_data = get_data_with_last_block_id(default_single_read_size,-1, &store, uid.clone()).await;
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(0, mem_data.shuffle_data_block_segments.get(0).unwrap().block_id);
        assert_eq!(1, mem_data.shuffle_data_block_segments.get(1).unwrap().block_id);

        // case2: when the last_block_id doesn't exist, it should return the data like when last_block_id=-1
        let mem_data = get_data_with_last_block_id(default_single_read_size,100, &store, uid.clone()).await;
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(0, mem_data.shuffle_data_block_segments.get(0).unwrap().block_id);
        assert_eq!(1, mem_data.shuffle_data_block_segments.get(1).unwrap().block_id);

        // case3: read from 3
        let mem_data = get_data_with_last_block_id(default_single_read_size, 3, &store, uid.clone()).await;
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(4, mem_data.shuffle_data_block_segments.get(0).unwrap().block_id);
        assert_eq!(5, mem_data.shuffle_data_block_segments.get(1).unwrap().block_id);

        // case4: some data are in inflight blocks
        let mut buffer = store.get_or_create_underlying_staging_buffer(uid.clone());
        let mut buffer = buffer.lock().await;
        let owned = buffer.staging.to_owned();
        buffer.staging.clear();
        let mut idx = 0;
        for block in owned {
            buffer.in_flight.insert(idx, vec![block]);
            idx += 1;
        }
        drop(buffer);

        // all data will be fetched from in_flight data
        let mem_data = get_data_with_last_block_id(default_single_read_size, 3, &store, uid.clone()).await;
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(4, mem_data.shuffle_data_block_segments.get(0).unwrap().block_id);
        assert_eq!(5, mem_data.shuffle_data_block_segments.get(1).unwrap().block_id);

        // case5: old data in in_flight and latest data in staging.
        // read it from the block id 9, and read size of 30
        let mut buffer = store.get_or_create_underlying_staging_buffer(uid.clone());
        let mut buffer = buffer.lock().await;
        buffer.staging.push(
            PartitionedDataBlock {
                block_id: 20,
                length: 10,
                uncompress_length: 0,
                crc: 0,
                data: BytesMut::with_capacity(10).freeze(),
                task_attempt_id: 0
            }
        );
        drop(buffer);

        let mem_data = get_data_with_last_block_id(30, 7, &store, uid.clone()).await;
        assert_eq!(3, mem_data.shuffle_data_block_segments.len());
        assert_eq!(8, mem_data.shuffle_data_block_segments.get(0).unwrap().block_id);
        assert_eq!(9, mem_data.shuffle_data_block_segments.get(1).unwrap().block_id);
        assert_eq!(20, mem_data.shuffle_data_block_segments.get(2).unwrap().block_id);

        // case6: read the end to return empty result
        let mem_data = get_data_with_last_block_id(30, 20, &store, uid.clone()).await;
        assert_eq!(0, mem_data.shuffle_data_block_segments.len());
    }

    async fn get_data_with_last_block_id(default_single_read_size: i64, last_block_id: i64, store: &MemoryStore, uid: PartitionedUId) -> PartitionedMemoryData {
        let ctx = ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                last_block_id, default_single_read_size
            )
        };
        if let Ok(data) = store.get(ctx).await {
            match data {
                mem(mem_data) => mem_data,
                _ => panic!()
            }
        } else {
            panic!();
        }
    }

    fn create_writing_ctx_with_blocks(block_number: i32, single_block_size: i32, uid: PartitionedUId) -> WritingViewContext {
        let mut data_blocks = vec![];
        for idx in 0..=9 {
            data_blocks.push(
                PartitionedDataBlock {
                    block_id: idx,
                    length: single_block_size.clone(),
                    uncompress_length: 0,
                    crc: 0,
                    data: BytesMut::with_capacity(single_block_size as usize).freeze(),
                    task_attempt_id: 0
                }
            );
        }
        WritingViewContext {
            uid,
            data_blocks
        }
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
}
