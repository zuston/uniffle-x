use std::borrow::BorrowMut;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::fmt::format;
use std::hash::Hash;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread::park;
use std::time::Duration;
use anyhow::{Result, anyhow};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;
use log::warn;
use tokio::{fs, select};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tonic::codegen::ok;
use tracing::dispatcher::set_default;
use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingViewContext, RequireBufferContext, WritingViewContext};
use crate::app::ReadingOptions::{MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE};
use crate::proto::uniffle::{ShuffleBlock, ShuffleData};
use crate::store::ResponseDataIndex::local;

#[derive(Debug)]
pub struct PartitionedData {
    pub partitionId: i32,
    pub blocks: Vec<PartitionedDataBlock>
}

#[derive(Debug, Clone)]
pub struct PartitionedDataBlock {
    pub block_id: i64,
    pub length: i32,
    pub uncompress_length: i32,
    pub crc: i64,
    pub data: Bytes,
    pub task_attempt_id: i64,
}

impl From<ShuffleData> for PartitionedData {
    fn from(shuffleData: ShuffleData) -> PartitionedData {

        let mut blocks = vec![];
        for data in shuffleData.block {
            let block = PartitionedDataBlock {
                block_id: data.block_id,
                length: data.length,
                uncompress_length: data.uncompress_length,
                crc: data.crc,
                data: data.data,
                task_attempt_id: data.task_attempt_id
            };
            blocks.push(block);
        }
        PartitionedData {
            partitionId: shuffleData.partition_id,
            blocks
        }
    }
}

pub enum ResponseDataIndex {
    local(LocalDataIndex)
}

pub struct LocalDataIndex {
    pub index_data: Bytes,
    pub data_file_len: i64,
}

pub enum ResponseData {
    local(PartitionedLocalData),
    mem(PartitionedMemoryData)
}

pub struct PartitionedLocalData {
    pub data: Bytes,
}

pub struct PartitionedMemoryData {
    pub shuffle_data_block_segments: Vec<DataSegment>,
    pub data: Bytes
}

pub struct DataSegment {
    pub block_id: i64,
    pub offset: i64,
    pub length: i32,
    pub uncompress_length: i32,
    pub crc: i64,
    pub task_attempt_id: i64,
}

// ================================================

#[derive(Debug, Clone)]
pub struct StagingBuffer {
    size: i64,
    blocks: Vec<PartitionedDataBlock>,
    in_flight: Vec<PartitionedDataBlock>
}

impl StagingBuffer {
    fn new() -> StagingBuffer {
        StagingBuffer {
            size: 0,
            blocks: vec![],
            in_flight: vec![]
        }
    }
}

#[derive(Clone)]
struct MemoryBudget {
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

    fn pre_allocate(&mut self, size: i64) -> Result<(bool, i64)> {
        let mut inner = self.inner.lock().unwrap();
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

    fn allocated_to_used(&mut self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if inner.allocated < size {
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        inner.used += size;
        Ok(true)
    }

    fn free_used(&mut self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if inner.used < size {
            inner.used = 0;
            // todo: metric
        } else {
            inner.used -= size;
        }
        Ok(true)
    }

    fn free_allocated(&mut self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if inner.allocated < size {
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        Ok(true)
    }
}

// ===========================================

use async_trait::async_trait;


#[async_trait]
pub trait Store {
    async fn insert(&mut self, ctx: WritingViewContext) -> Result<()>;
    async fn get(&mut self, ctx: ReadingViewContext) -> Result<ResponseData>;
    async fn get_index(&mut self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex>;
    async fn require_buffer(&mut self, ctx: RequireBufferContext) -> Result<(bool, i64)>;
    async fn purge(&mut self, app_id: String) -> Result<()>;
}

// ===========================================

fn create_directory_if_not_exists(dir_path: &str) {
    if !std::fs::metadata(dir_path).is_ok() {
        std::fs::create_dir_all(dir_path);
    }
}

pub struct LocalFileStore {
    local_disks: Vec<Arc<LocalDisk>>,
    partition_written_disk_map: DashMap<PartitionedUId, Arc<LocalDisk>>,
    file_locks: DashMap<String, RwLock<()>>,
}

impl LocalFileStore {
    fn new(local_disks: Vec<String>) -> Self {
        let mut local_disk_instances = vec![];
        for path in local_disks {
            local_disk_instances.push(
                Arc::new(LocalDisk::new(path))
            );
        }
        LocalFileStore {
            local_disks: local_disk_instances,
            partition_written_disk_map: DashMap::new(),
            file_locks: DashMap::new()
        }
    }

    fn gen_relative_path(uid: &PartitionedUId) -> (String, String) {
        (
            format!("{}/{}/partition-{}.data", uid.app_id, uid.shuffle_id, uid.partition_id),
            format!("{}/{}/partition-{}.index", uid.app_id, uid.shuffle_id, uid.partition_id)
        )
    }
}

#[async_trait]
impl Store for LocalFileStore {
    async fn insert(&mut self, ctx: WritingViewContext) -> Result<()> {
        let uid = ctx.uid;
        let (data_file_path, index_file_path) = LocalFileStore::gen_relative_path(&uid);
        let mut local_disk = self.partition_written_disk_map.entry(uid).or_insert_with(||{
            // todo: support hash selection or more pluggable selections
            self.local_disks.get(0).unwrap().clone()
        }).clone();

        self.file_locks.entry(data_file_path.clone()).or_insert_with(|| RwLock::new(()))
            .write().unwrap();

        // write index file and data file
        for block in ctx.data_blocks {
            let next_offset = local_disk.next_offset(&data_file_path);

            let block_id = block.block_id;
            let length = block.length;
            let uncompress_len = block.uncompress_length;
            let task_attempt_id = block.task_attempt_id;
            let crc = block.crc;
            let data = block.data;

            let data_file_cloned = data_file_path.clone();

            // let mut cloned_local_disk = local_disk.clone();
            // let future = tokio::spawn(async move {
            //    cloned_local_disk.write(data, data_file_cloned).await?
            // });
        }

        Ok(())
    }

    async fn get(&mut self, ctx: ReadingViewContext) -> Result<ResponseData> {
        todo!()
    }

    async fn get_index(&mut self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex> {
        todo!()
    }

    async fn require_buffer(&mut self, ctx: RequireBufferContext) -> Result<(bool, i64)> {
        panic!("It should happen")
    }

    async fn purge(&mut self, app_id: String) -> Result<()> {
        todo!()
    }
}

struct LocalDisk {
    base_path: String,
    partition_file_len: HashMap<String, i64>
}

impl LocalDisk {
    fn new(path: String) -> Self {
        create_directory_if_not_exists(&path);
        LocalDisk {
            base_path: path,
            partition_file_len: HashMap::new()
        }
    }

    fn append_path(&self, path: String) -> String {
        format!("{}/{}", self.base_path.clone(), path)
    }

    fn next_offset(&self, data_file_name: &str) -> i64 {
        self.partition_file_len.get(data_file_name).copied().unwrap_or(0)
    }

    async fn write(&mut self, data: Bytes, relative_file_path: String) -> Result<()> {
        let absolute_path = self.append_path(relative_file_path.clone());
        let path = Path::new(&absolute_path);

        match path.parent() {
            Some(parent) => {
                if !parent.exists() {
                    create_directory_if_not_exists(parent.to_str().unwrap())
                }
            },
            _ => todo!()
        }

        let mut output_file= OpenOptions::new().append(true).create(true).open(absolute_path).await?;
        output_file.write_all(data.as_ref()).await?;

        let mut val = self.partition_file_len.entry(relative_file_path).or_insert(0);
        *val += data.len() as i64;

        Ok(())
    }

    async fn read(&self, relative_file_path: String, offset: i64, length: i64) -> Result<Bytes> {
        let file_path = self.append_path(relative_file_path);

        let file = tokio::fs::File::open(&file_path).await?;
        let mut reader = tokio::io::BufReader::new(file);

        let mut buffer = vec![0; length as usize];
        reader.seek(SeekFrom::Start(offset as u64)).await?;
        reader.read_exact(buffer.as_mut()).await?;

        Ok(Bytes::copy_from_slice(&*buffer))
    }
}

// ===========================================

#[derive(Clone)]
pub struct MemoryStore {
    // todo: change to RW lock
    state: DashMap<String, DashMap<i32, DashMap<i32, Arc<Mutex<StagingBuffer>>>>>,
    budget: MemoryBudget,
    memory_allocated_of_app: DashMap<String, i64>
}

unsafe impl Send for MemoryStore {}
unsafe impl Sync for MemoryStore {}

impl MemoryStore {
    pub fn new(max_memory_size: i64) -> Self {
        MemoryStore {
            state: DashMap::new(),
            budget: MemoryBudget::new(max_memory_size),
            memory_allocated_of_app: DashMap::new()
        }
    }

    pub fn get_or_create_underlying_staging_buffer(&mut self, uid: PartitionedUId) -> Arc<Mutex<StagingBuffer>> {
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
    async fn insert(&mut self, ctx: WritingViewContext) -> Result<()> {
        let uid = ctx.uid;
        let buffer = self.get_or_create_underlying_staging_buffer(uid);
        let mut buffer = buffer.lock().unwrap();

        let blocks = ctx.data_blocks;
        
        let mut added_size = 0i64;
        for block in blocks {
            added_size += block.length as i64;
            buffer.blocks.push(block);
        }

        buffer.size += added_size;
        Ok(())
    }

    async fn get(&mut self, ctx: ReadingViewContext) -> Result<ResponseData> {
        let uid = ctx.uid;
        let buffer = self.get_or_create_underlying_staging_buffer(uid);
        let mut buffer = buffer.lock().unwrap();

        let options = ctx.reading_options;
        let (fetched_blocks, length) = match options {
            MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(last_block_id, max_size) => {
                // if the first time, it should read from blocks rather than in flight
                let mut fetched = vec![];
                let mut fetched_size = 0;
                if last_block_id != -1 {
                    let mut existence = false;
                    for in_flight_block in &buffer.in_flight {
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
        let result = self.budget.pre_allocate(ctx.size);
        if result.is_ok() {
            let mut val = self.memory_allocated_of_app.entry(ctx.uid.app_id).or_insert_with(||0);
            *val += ctx.size;
        }
        result
    }

    async fn purge(&mut self, app_id: String) -> Result<()> {
        self.memory_allocated_of_app.get(&app_id).map(|v| {
            let size = v.value();
            self.budget.free_allocated(*size);
        });

        self.state.remove(&app_id);
        Ok(())
    }
}

// ===========================================

#[cfg(test)]
mod test {
    use core::panic;
    use std::borrow::Borrow;
    use std::io::Read;
    use async_trait::async_trait;
    use bytes::{Bytes, BytesMut};
    use log::Level::Debug;
    use crate::app::{PartitionedUId, ReadingOptions, ReadingViewContext, RequireBufferContext, WritingViewContext};
    use crate::store::{LocalDisk, MemoryStore, PartitionedDataBlock, ResponseData, Store};
    use crate::store::ResponseDataIndex::local;

    #[tokio::test]
    async fn local_disk_test() {
        let temp_dir = tempdir::TempDir::new("test_directory").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        let mut local_disk = LocalDisk::new(temp_path.clone());

        let data = b"Hello, World!";

        let relative_path = "app-id/test_file.txt";
        let write_result = local_disk.write(Bytes::copy_from_slice(data), relative_path.to_string()).await;
        assert!(write_result.is_ok());

        // test whether the content is written
        let file_path = format!("{}/{}", local_disk.base_path, relative_path);
        let mut file = std::fs::File::open(file_path).unwrap();
        let mut file_content = Vec::new();
        file.read_to_end(&mut file_content).unwrap();
        assert_eq!(file_content, data);

        // if the file has been created, append some content
        let write_result = local_disk.write(Bytes::copy_from_slice(data), relative_path.to_string()).await;
        assert!(write_result.is_ok());

        let read_result = local_disk.read(relative_path.to_string(), 0, data.len() as i64 * 2).await;
        assert!(read_result.is_ok());
        let read_data = read_result.unwrap();
        let expected = b"Hello, World!Hello, World!";
        assert_eq!(read_data.as_ref(), expected);

        temp_dir.close().unwrap();
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
                store.purge("100".to_string());
            }
            _ => panic!()
        }

        let budget = store.budget.inner.lock().unwrap();
        assert_eq!(0, budget.allocated);
        assert_eq!(0, budget.used);
        assert_eq!(1024 * 1024 * 1024, budget.capacity);

        assert_eq!(false, store.state.contains_key("100".into()));
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
}
