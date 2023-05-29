use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc};
use std::time::Duration;
use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext, RequireBufferContext, WritingViewContext};
use crate::store::memory::{MemoryStore, StagingBuffer};
use crate::store::{ResponseData, ResponseDataIndex, Store};
use crate::store::localfile::LocalFileStore;
use async_trait::async_trait;
use anyhow::{Result, anyhow};
use async_channel::TryRecvError;
use log::{debug, error, info};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, Mutex, oneshot};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tracing::field::debug;
use crate::config;
use crate::config::{Config, HybridStoreConfig, LocalfileStoreConfig};
use crate::store::ResponseData::mem;

pub struct HybridStore {
    // Box<dyn Store> will build fail
    hot_store: Box<MemoryStore>,
    warm_store: Box<LocalFileStore>,
    config: HybridStoreConfig,
    memory_spill_lock: Mutex<()>,
    memory_spill_recv: async_channel::Receiver<SpillMessage>,
    memory_spill_send: async_channel::Sender<SpillMessage>
}

struct SpillMessage {
    ctx: WritingViewContext,
    id: i64
}

unsafe impl Send for HybridStore {}
unsafe impl Sync for HybridStore {}

impl HybridStore {
    pub fn from(config: Config) -> Self {
        let (send, recv) = async_channel::unbounded();
        let mut store = HybridStore {
            hot_store: Box::new(MemoryStore::from(config.memory_store.unwrap())),
            warm_store: Box::new(LocalFileStore::from(config.localfile_store.unwrap())),
            config: config.hybrid_store.unwrap(),
            memory_spill_lock: Mutex::new(()),
            memory_spill_recv: recv,
            memory_spill_send: send
        };
        store
    }

    async fn memory_spill_to_localfile(&self, ctx: WritingViewContext, in_flight_blocks_id: i64) -> Result<String> {
        info!("Doing spill");
        let uid = ctx.uid.clone();
        let blocks = &ctx.data_blocks;
        let mut spill_size = 0i64;
        for block in blocks {
            spill_size += block.length as i64;
        }

        self.warm_store.insert(ctx).await?;
        self.hot_store.release_in_flight_blocks_in_underlying_staging_buffer(uid, in_flight_blocks_id).await?;
        self.hot_store.free_memory(spill_size).await?;

        Ok(format!("writing view context: {}, flush size: {}", 1, in_flight_blocks_id))
    }
}

#[async_trait]
impl Store for HybridStore {
    /// Using the async_channel to keep the immutable self to
    /// the self as the Arc<xxx> rather than mpsc::channel, which
    /// uses the recv(&mut self). I don't hope so.
    fn start(self: Arc<HybridStore>) {
        let store = self.clone();
        tokio::spawn(async move {
            while let Ok(message) = store.memory_spill_recv.recv().await {
                info!("Accepted spill event...");
                let store_cloned = store.clone();
                tokio::spawn(async move {
                    match store_cloned.memory_spill_to_localfile(message.ctx, message.id).await {
                        Ok(msg) => info!("{}", msg),
                        Err(error) => error!("Errors on spilling memory data to localfile. error: {:#?}", error)
                    }
                });
            }
        });
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<()> {
        let insert_result = self.hot_store.insert(ctx).await;
        let spill_lock = self.memory_spill_lock.lock().await;
        let used_ratio = self.hot_store.memory_usage_ratio().await;
        debug!("used ratio: {}", used_ratio);
        if used_ratio > self.config.memory_spill_high_watermark {
            let target_size = (self.hot_store.memory_capacity as f32 * self.config.memory_spill_low_watermark) as i64;
            let buffers = self.hot_store.get_required_spill_buffer(target_size).await;

            for (partition_id, buffer) in buffers {
                let mut buffer_inner = buffer.lock().await;
                let blocks = buffer_inner.to_owned().staging;
                let writingCtx = WritingViewContext {
                    uid: partition_id,
                    data_blocks: blocks.clone()
                };
                let in_flight_block_id = buffer_inner.add_blocks_to_send(blocks).unwrap();
                buffer_inner.staging.clear();

                if self.memory_spill_send.send(SpillMessage {
                    ctx: writingCtx,
                    id: in_flight_block_id
                }).await.is_err() {
                    error!("Errors on sending spill message to queue. This should not happen.");
                }
            }
            info!("Doing spilling in background....");
        }
        insert_result
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData> {
        match ctx.reading_options {
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(_, _) => self.hot_store.get(ctx).await,
            _ => self.warm_store.get(ctx).await
        }
    }

    async fn get_index(&self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex> {
        self.warm_store.get_index(ctx).await
    }

    async fn require_buffer(&self, ctx: RequireBufferContext) -> Result<(bool, i64)> {
        self.hot_store.require_buffer(ctx).await
    }

    async fn purge(&self, app_id: String) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::read;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use bytes::{Buf, Bytes};
    use log::info;
    use tokio::time;
    use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext, RequireBufferContext, WritingViewContext};
    use crate::config::{Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig, StorageType};
    use crate::store::hybrid::HybridStore;
    use crate::store::{PartitionedDataBlock, ResponseData, ResponseDataIndex, Store};
    use crate::store::ResponseData::mem;

    fn start_store() -> Arc<HybridStore> {
        let data = b"hello world!";
        let data_len = data.len();

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let mut config = Config::default();
        config.memory_store=Some(MemoryStoreConfig {
            capacity: (data_len * 1) as i64
        });
        config.localfile_store = Some(LocalfileStoreConfig {
            data_paths: vec![temp_path]
        });
        config.hybrid_store = Some(HybridStoreConfig {
            memory_spill_high_watermark: 0.8,
            memory_spill_low_watermark: 0.2
        });
        config.store_type = Some(StorageType::MEMORY_LOCALFILE);

        /// The hybrid store will flush the memory data to file when
        /// the data reaches the number of 4

        let mut store = Arc::new(HybridStore::from(config));
        store
    }

    async fn write_some_data(store: Arc<HybridStore>, uid: PartitionedUId, data_len: i32, data: &[u8; 12]) {
        for i in 0..=3 {
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
    }

    #[tokio::test]
    async fn get_data_from_localfile() {
        let data = b"hello world!";
        let data_len = data.len();

        let mut store = start_store();
        store.clone().start();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0
        };
        write_some_data(store.clone(), uid.clone(), data_len as i32, data).await;
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

        let mut store = start_store();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0
        };
        write_some_data(store.clone(), uid.clone(), data_len as i32, data).await;
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
