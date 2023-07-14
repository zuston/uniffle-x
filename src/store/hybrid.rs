use std::any::Any;
use std::borrow::BorrowMut;
use std::cmp::max;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::str::FromStr;
use std::sync::{Arc};
use std::time::Duration;
use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext, RequireBufferContext, WritingViewContext};
use crate::store::memory::{MemorySnapshot, MemoryStore, StagingBuffer};
use crate::store::{Persistent, ResponseData, ResponseDataIndex, Store};
use crate::store::localfile::LocalFileStore;
use async_trait::async_trait;
use anyhow::{Result, anyhow, Error};
use async_channel::TryRecvError;
use await_tree::Registry;
use log::{debug, error, info};
use prometheus::core::{Atomic, AtomicU64};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, Mutex, MutexGuard, oneshot, Semaphore};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tracing::field::debug;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::config;
use crate::config::{Config, HdfsStoreConfig, HybridStoreConfig, LocalfileStoreConfig, StorageType};
use crate::error::DatanodeError;
use crate::metric::{GAUGE_MEMORY_SPILL_OPERATION, GAUGE_MEMORY_SPILL_TO_HDFS, GAUGE_MEMORY_SPILL_TO_LOCALFILE, TOTAL_MEMORY_SPILL_OPERATION, TOTAL_MEMORY_SPILL_OPERATION_FAILED, TOTAL_MEMORY_SPILL_TO_HDFS, TOTAL_MEMORY_SPILL_TO_LOCALFILE};
use crate::readable_size::ReadableSize;
use crate::store::hdfs::HdfsStore;
use crate::store::ResponseData::mem;

trait PersistentStore: Store + Persistent + Send + Sync {}
impl PersistentStore for LocalFileStore {}
impl PersistentStore for HdfsStore {}

const DEFAULT_MEMORY_SPILL_MAX_CONCURRENCY: i32 = 20;

pub struct HybridStore {
    // Box<dyn Store> will build fail
    hot_store: Box<MemoryStore>,

    warm_store: Option<Box<dyn PersistentStore>>,
    cold_store: Option<Box<dyn PersistentStore>>,

    config: HybridStoreConfig,
    memory_spill_lock: Mutex<()>,
    memory_spill_recv: async_channel::Receiver<SpillMessage>,
    memory_spill_send: async_channel::Sender<SpillMessage>,
    memory_spill_event_num: AtomicU64,

    memory_spill_to_cold_threshold_size: Option<u64>,

    memory_spill_max_concurrency: i32,
}

struct SpillMessage {
    ctx: WritingViewContext,
    id: i64
}

unsafe impl Send for HybridStore {}
unsafe impl Sync for HybridStore {}


impl HybridStore {
    pub fn from(config: Config) -> Self {
        let store_type = &config.store_type.unwrap_or(StorageType::MEMORY);
        if !StorageType::contains_memory(&store_type) {
            panic!("Storage type must contains memory.");
        }

        let mut persistent_stores: VecDeque<Box<dyn PersistentStore>> = VecDeque::with_capacity(2);
        if StorageType::contains_localfile(&store_type) {
            let localfile_store = LocalFileStore::from(config.localfile_store.unwrap());
            persistent_stores.push_back(
                Box::new(localfile_store)
            );
        }
        if StorageType::contains_hdfs(&store_type) {
            let hdfs_store = HdfsStore::from(config.hdfs_store.unwrap());
            persistent_stores.push_back(
                Box::new(hdfs_store)
            );
        }

        let (send, recv) = async_channel::unbounded();
        let hybrid_conf = config.hybrid_store.unwrap();
        let memory_spill_to_cold_threshold_size = match &hybrid_conf.memory_spill_to_cold_threshold_size {
            Some(v) => Some(ReadableSize::from_str(&v.clone()).unwrap().as_bytes()),
            _ => None
        };
        let memory_spill_max_concurrency = hybrid_conf.memory_spill_max_concurrency.unwrap_or(DEFAULT_MEMORY_SPILL_MAX_CONCURRENCY);

        let mut store = HybridStore {
            hot_store: Box::new(MemoryStore::from(config.memory_store.unwrap())),
            warm_store: persistent_stores.pop_front(),
            cold_store: persistent_stores.pop_front(),
            config: hybrid_conf,
            memory_spill_lock: Mutex::new(()),
            memory_spill_recv: recv,
            memory_spill_send: send,
            memory_spill_event_num: AtomicU64::new(0),
            memory_spill_to_cold_threshold_size,
            memory_spill_max_concurrency,
        };
        store
    }

    fn is_memory_only(&self) -> bool {
        self.cold_store.is_none() && self.warm_store.is_none()
    }

    fn get_store_type(&self, store: &dyn Any) -> StorageType {
        if store.is::<LocalFileStore>() {
            return StorageType::LOCALFILE;
        }
        if store.is::<HdfsStore>() {
            return StorageType::HDFS;
        }
        return StorageType::MEMORY;
    }

    fn is_localfile(&self, store: &dyn Any) -> bool {
        store.is::<LocalFileStore>()
    }

    fn is_hdfs(&self, store: &dyn Any) -> bool {
        store.is::<HdfsStore>()
    }

    async fn memory_spill_to_persistent_store(&self, ctx: WritingViewContext, in_flight_blocks_id: i64) -> Result<String> {
        let uid = ctx.uid.clone();
        let blocks = &ctx.data_blocks;
        let mut spill_size = 0i64;
        for block in blocks {
            spill_size += block.length as i64;
        }

        let warm = self.warm_store.as_ref().ok_or(anyhow!("empty warm store. It should not happen"))?;
        let cold = self.cold_store.as_ref().unwrap_or(warm);

        let candidate_store = if warm.is_healthy().await? {
            let cold_spilled_size = self.memory_spill_to_cold_threshold_size.unwrap_or(u64::MAX);
            if cold_spilled_size < spill_size as u64 {
                cold
            } else {
                warm
            }
        } else {
            cold
        };

        match self.get_store_type(candidate_store) {
            StorageType::LOCALFILE => {
                TOTAL_MEMORY_SPILL_TO_LOCALFILE.inc();
                GAUGE_MEMORY_SPILL_TO_LOCALFILE.inc();
            },
            StorageType::HDFS => {
                TOTAL_MEMORY_SPILL_TO_HDFS.inc();
                GAUGE_MEMORY_SPILL_TO_HDFS.inc();
            },
            _ => {}
        }

        let message = format!("partition uid: {:?}, memory spilled size: {}", &ctx.uid, spill_size);

        candidate_store.insert(ctx).await?;
        self.hot_store.release_in_flight_blocks_in_underlying_staging_buffer(uid, in_flight_blocks_id).await?;
        self.hot_store.free_memory(spill_size).await?;

        match self.get_store_type(candidate_store) {
            StorageType::LOCALFILE => {
                GAUGE_MEMORY_SPILL_TO_LOCALFILE.dec();
            },
            StorageType::HDFS => {
                GAUGE_MEMORY_SPILL_TO_HDFS.dec();
            },
            _ => {}
        }

        Ok(message)
    }

    pub async fn get_hot_store_memory_snapshot(&self) -> Result<MemorySnapshot> {
        self.hot_store.memory_snapshot().await
    }

    pub async fn get_hot_store_memory_partitioned_buffer_size(&self, uid: &PartitionedUId) -> Result<u64> {
        self.hot_store.get_partitioned_buffer_size(uid).await
    }

    pub fn memory_spill_event_num(&self) -> Result<u64> {
        Ok(
            self.memory_spill_event_num.get()
        )
    }

    async fn make_memory_buffer_flush(&self, buffer_inner: &mut MutexGuard<'_, StagingBuffer>, uid: PartitionedUId) -> Result<()> {
        let (in_flight_uid, blocks) = buffer_inner.migrate_staging_to_in_flight()?;

        let writingCtx = WritingViewContext {
            uid,
            data_blocks: blocks
        };

        if self.memory_spill_send.send(SpillMessage {
            ctx: writingCtx,
            id: in_flight_uid
        }).await.is_err() {
            error!("Errors on sending spill message to queue. This should not happen.");
        } else {
            self.memory_spill_event_num.inc_by(1);
        }

        Ok(())
    }
}

#[async_trait]
impl Store for HybridStore {
    /// Using the async_channel to keep the immutable self to
    /// the self as the Arc<xxx> rather than mpsc::channel, which
    /// uses the recv(&mut self). I don't hope so.
    fn start(self: Arc<HybridStore>) {
        if self.is_memory_only() {
            return;
        }

        let await_tree_registry: Arc<Mutex<Registry<u64>>> = AWAIT_TREE_REGISTRY.clone();

        let store = self.clone();
        let concurrency_limiter = Arc::new(Semaphore::new(store.memory_spill_max_concurrency as usize));
        tokio::spawn(async move {
            let mut counter = 0u64;
            while let Ok(message) = store.memory_spill_recv.recv().await {
                let mut registry = await_tree_registry.lock().await;
                let await_root = registry.register(counter, format!("actor: {}", counter));
                counter += 1;

                // using acquire_owned(), refer to https://github.com/tokio-rs/tokio/issues/1998
                let concurrency_guarder = concurrency_limiter.clone().acquire_owned().await.unwrap();

                TOTAL_MEMORY_SPILL_OPERATION.inc();
                GAUGE_MEMORY_SPILL_OPERATION.inc();
                let store_cloned = store.clone();
                tokio::spawn(await_root.instrument(async move {
                    match store_cloned.memory_spill_to_persistent_store(message.ctx, message.id).await {
                        Ok(msg) => debug!("{}", msg),
                        Err(error) => {
                            TOTAL_MEMORY_SPILL_OPERATION_FAILED.inc();
                            error!("Errors on spilling memory data to localfile. error: {:#?}", error)
                        }
                    }
                    store_cloned.memory_spill_event_num.dec_by(1);
                    GAUGE_MEMORY_SPILL_OPERATION.dec();
                    drop(concurrency_guarder);
                }));
            }
        });
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), DatanodeError> {
        let uid = ctx.uid.clone();

        let insert_result = self.hot_store.insert(ctx).await;
        if self.is_memory_only() {
            return insert_result;
        }

        let spill_lock = self.memory_spill_lock.lock().await;

        // single buffer flush
        let buffer = self.hot_store.get_or_create_underlying_staging_buffer(uid.clone());
        let mut buffer_inner = buffer.lock().await;
        let max_spill_size = &self.config.memory_single_buffer_max_spill_size;
        if max_spill_size.is_some() {
            match ReadableSize::from_str(max_spill_size.clone().unwrap().as_str()) {
                Ok(size) => {
                    if size.as_bytes() < buffer_inner.get_staging_size()? as u64 {
                        self.make_memory_buffer_flush(&mut buffer_inner, uid.clone()).await?;
                    }
                },
                _ => {}
            }
        }
        drop(buffer_inner);

        // watermark flush
        let used_ratio = self.hot_store.memory_usage_ratio().await;
        if used_ratio > self.config.memory_spill_high_watermark {
            let target_size = (self.hot_store.get_capacity()? as f32 * self.config.memory_spill_low_watermark) as i64;
            let buffers = self.hot_store.get_required_spill_buffer(target_size).await;

            for (partition_id, buffer) in buffers {
                let mut buffer_inner = buffer.lock().await;
                self.make_memory_buffer_flush(&mut buffer_inner, partition_id).await?;
            }
            debug!("Trigger spilling in background....");
        }
        insert_result
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, DatanodeError> {
        match ctx.reading_options {
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(_, _) => self.hot_store.get(ctx).await,
            _ => self.warm_store.as_ref().unwrap().get(ctx).await
        }
    }

    async fn get_index(&self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex, DatanodeError> {
        self.warm_store.as_ref().unwrap().get_index(ctx).await
    }

    async fn require_buffer(&self, ctx: RequireBufferContext) -> Result<(bool, i64)> {
        self.hot_store.require_buffer(ctx).await
    }

    async fn purge(&self, app_id: String) -> Result<()> {
        self.hot_store.purge(app_id.clone()).await?;
        if self.warm_store.is_some() {
            self.warm_store.as_ref().unwrap().purge(app_id.clone()).await?;
        }
        if self.cold_store.is_some() {
            self.cold_store.as_ref().unwrap().purge(app_id.clone()).await?;
        }
        Ok(())
    }

    async fn is_healthy(&self) -> Result<bool> {
        async fn check_healthy(store: Option<&Box<dyn PersistentStore>>) -> Result<bool> {
            match store {
                Some(store) => store.is_healthy().await,
                _ => Ok(true)
            }
        }
        let warm = check_healthy(self.warm_store.as_ref()).await.unwrap_or(false);
        let cold = check_healthy(self.cold_store.as_ref()).await.unwrap_or(false);
        Ok(
            self.hot_store.is_healthy().await? && (warm || cold)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs::read;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use bytes::{Buf, Bytes, BytesMut};
    use log::info;
    use tokio::time;
    use std::any::Any;
    use crate::app::{App, PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext, RequireBufferContext, WritingViewContext};
    use crate::app::ReadingOptions::{FILE_OFFSET_AND_LEN, MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE};
    use crate::config::{Config, HdfsStoreConfig, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig, StorageType};
    use crate::store::hybrid::{HybridStore, PersistentStore};
    use crate::store::{PartitionedDataBlock, ResponseData, ResponseDataIndex, Store};
    use crate::store::hdfs::HdfsStore;
    use crate::store::ResponseData::{local, mem};

    #[test]
    fn type_downcast_check() {
        trait Fruit {}

        struct Banana {}
        impl Fruit for Banana {}

        struct Apple {}
        impl Fruit for Apple {}

        fn is_apple(store: &dyn Any) -> bool {
            store.is::<Apple>()
        }

        assert_eq!(true, is_apple(&Apple {}));
        assert_eq!(false, is_apple(&Banana {}));
    }

    #[tokio::test]
    async fn test_only_memory() {
        let mut config = Config::default();
        config.memory_store=Some(MemoryStoreConfig {
            capacity: "20M".to_string()
        });
        config.hybrid_store = Some(HybridStoreConfig::new(
            0.8,
            0.2,
            None
        ));
        config.store_type = Some(StorageType::MEMORY);
        let mut store = HybridStore::from(config);
        assert_eq!(true, store.is_healthy().await.unwrap());
    }

    #[test]
    fn test_vec_pop() {
        let mut stores = VecDeque::with_capacity(2);
        stores.push_back(1);
        stores.push_back(2);
        assert_eq!(1, stores.pop_front().unwrap());
        assert_eq!(2, stores.pop_front().unwrap());
        assert_eq!(None, stores.pop_front());
    }

    fn start_store(memory_single_buffer_max_spill_size: Option<String>, memory_capacity: String) -> Arc<HybridStore> {
        let data = b"hello world!";
        let data_len = data.len();

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let mut config = Config::default();
        config.memory_store=Some(MemoryStoreConfig {
            capacity: memory_capacity
        });
        config.localfile_store = Some(LocalfileStoreConfig::new(
            vec![temp_path]
        ));
        config.hybrid_store = Some(HybridStoreConfig::new(
            0.8,
            0.2,
            memory_single_buffer_max_spill_size
        ));
        config.store_type = Some(StorageType::MEMORY_LOCALFILE);

        /// The hybrid store will flush the memory data to file when
        /// the data reaches the number of 4

        let mut store = Arc::new(HybridStore::from(config));
        store
    }

    async fn write_some_data(store: Arc<HybridStore>, uid: PartitionedUId, data_len: i32, data: &[u8; 12], batch_size: i64) -> Vec<i64> {
        let mut block_ids = vec![];
        for i in 0..batch_size {
            block_ids.push(i);
            let writingCtx = WritingViewContext {
                uid: uid.clone(),
                data_blocks: vec![
                    PartitionedDataBlock {
                        block_id: i,
                        length: data_len as i32,
                        uncompress_length: 100,
                        crc: 0,
                        data: Bytes::copy_from_slice(data),
                        task_attempt_id: 0
                    }
                ]
            };
            let _ = store.insert(writingCtx).await;
        }

        block_ids
    }

    #[tokio::test]
    async fn single_buffer_spill_test() -> anyhow::Result<()> {
        let data = b"hello world!";
        let data_len = data.len();

        let mut store = start_store(Some("1".to_string()), ((data_len * 10000) as i64).to_string());
        store.clone().start();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0
        };
        let expected_block_ids = write_some_data(store.clone(), uid.clone(), data_len as i32, data, 100).await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        // read from memory and then from localfile
        let response_data = store.get(ReadingViewContext {
            uid: uid.clone(),
            reading_options: MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1024 * 1024 * 1024)
        }).await?;

        let mut accepted_block_ids = vec![];
        for segment in response_data.from_memory().shuffle_data_block_segments {
            accepted_block_ids.push(segment.block_id);
        }
        
        let local_index_data = store.get_index(ReadingIndexViewContext {
            partition_id: uid.clone()
        }).await?;

        match local_index_data {
            ResponseDataIndex::local(index) => {
                let mut index_bytes = index.index_data;
                while index_bytes.has_remaining() {
                    // index_bytes_holder.put_i64(next_offset);
                    // index_bytes_holder.put_i32(length);
                    // index_bytes_holder.put_i32(uncompress_len);
                    // index_bytes_holder.put_i64(crc);
                    // index_bytes_holder.put_i64(block_id);
                    // index_bytes_holder.put_i64(task_attempt_id);
                    index_bytes.get_i64();
                    index_bytes.get_i32();
                    index_bytes.get_i32();
                    index_bytes.get_i64();
                    let id = index_bytes.get_i64();
                    index_bytes.get_i64();

                    accepted_block_ids.push(id);
                }
            },
            _ => panic!("")
        }

        assert_eq!(accepted_block_ids, expected_block_ids);

        Ok(())
    }

    #[tokio::test]
    async fn get_data_from_localfile() {
        let data = b"hello world!";
        let data_len = data.len();

        let mut store = start_store(None, ((data_len * 1) as i64).to_string());
        store.clone().start();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0
        };
        write_some_data(store.clone(), uid.clone(), data_len as i32, data, 4).await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        // case1: all data has been flushed to localfile. the data in memory should be empty
        let mut last_block_id = -1;
        let readingViewCtx = ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(last_block_id, data_len as i64)
        };

        let read_data = store.get(readingViewCtx).await;
        if read_data.is_err() {
            panic!();
        }
        let read_data = read_data.unwrap();
        match read_data {
            mem(mem_data) => {
                assert_eq!(0, mem_data.shuffle_data_block_segments.len());
            }
            _ => panic!()
        }

        // case2: read data from localfile
        // 1. read index file
        // 2. read data
        let indexViewCtx = ReadingIndexViewContext {
            partition_id: uid.clone()
        };
        match store.get_index(indexViewCtx).await.unwrap() {
            ResponseDataIndex::local(index) => {
                let mut index_data = index.index_data;
                while index_data.has_remaining() {
                    let offset = index_data.get_i64();
                    let length = index_data.get_i32();
                    let uncompress = index_data.get_i32();
                    let crc = index_data.get_i64();
                    let block_id = index_data.get_i64();
                    let task_id = index_data.get_i64();

                    let readingViewCtx = ReadingViewContext {
                        uid: uid.clone(),
                        reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(offset, length as i64)
                    };
                    let read_data = store.get(readingViewCtx).await.unwrap();
                    match read_data {
                        ResponseData::local(local_data) => {
                            assert_eq!(Bytes::copy_from_slice(data), local_data.data);
                        },
                        _ => panic!()
                    }
                }
            },
            _ => panic!()
        }
    }

    #[tokio::test]
    async fn test_insert_and_get_from_memory() {
        let data = b"hello world!";
        let data_len = data.len();

        let mut store = start_store(None, ((data_len * 1) as i64).to_string());

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0
        };
        write_some_data(store.clone(), uid.clone(), data_len as i32, data, 4).await;
        let mut last_block_id = -1;
        // read data one by one
        for idx in 0..=10 {
            let readingViewCtx = ReadingViewContext {
                uid: uid.clone(),
                reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(last_block_id, data_len as i64)
            };

            let read_data = store.get(readingViewCtx).await;
            if read_data.is_err() {
                panic!();
            }

            match read_data.unwrap() {
                mem(mem_data) => {
                    if idx >= 4 {
                        println!("idx: {}, len: {}", idx, mem_data.shuffle_data_block_segments.len());
                        continue;
                    }
                    assert_eq!(Bytes::copy_from_slice(data), mem_data.data);
                    let segments = mem_data.shuffle_data_block_segments;
                    assert_eq!(1, segments.len());
                    last_block_id = segments.get(0).unwrap().block_id;
                },
                _ => panic!()
            }
        }
    }
}
