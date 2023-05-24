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
use log::{error, info};
use tokio::sync::{mpsc, Mutex, oneshot};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use crate::config;
use crate::config::{Config, HybridStoreConfig, LocalfileStoreConfig};
use crate::store::ResponseData::mem;

struct HybridStore {
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
    fn from(config: Config) -> Self {
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

    /// Using the async_channel to keep the immutable self to
    /// the self as the Arc<xxx> rather than mpsc::channel, which
    /// uses the recv(&mut self). I don't hope so.
    fn start_flush_in_background(self: Arc<HybridStore>) {
        let store = self.clone();
        tokio::spawn(async move {
            while let Ok(message) = store.memory_spill_recv.recv().await {
                match store.memory_spill_to_localfile(message.ctx, message.id).await {
                    Ok(msg) => info!("{}", msg),
                    Err(error) => error!("Errors on spilling memory data to localfile. error: {:#?}", error)
                }
            }
        });
    }

    async fn memory_spill_to_localfile(&self, ctx: WritingViewContext, in_flight_blocks_id: i64) -> Result<String> {
        println!("Doing spill");
        let uid = ctx.uid.clone();
        let blocks = &ctx.data_blocks;
        let mut spill_size = 0i64;
        for block in blocks {
            spill_size += block.length as i64;
        }
        let result = self.warm_store.insert(ctx).await;
        let _ = self.hot_store.release_in_flight_blocks_in_underlying_staging_buffer(uid, in_flight_blocks_id).await;
        self.hot_store.free_memory(spill_size).await.expect("TODO: panic message");

        Ok(format!("writing view context: {}, flush size: {}", 1, in_flight_blocks_id))
    }
}

#[async_trait]
impl Store for HybridStore {
    // todo: check the consistency problems
    async fn insert(&self, ctx: WritingViewContext) -> Result<()> {
        let insert_result = self.hot_store.insert(ctx).await;
        let spill_lock = self.memory_spill_lock.lock().await;
        let used_ratio = self.hot_store.memory_usage_ratio().await;
        println!("used ratio: {}", used_ratio);
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

                let _ = self.memory_spill_send.send(SpillMessage {
                    ctx: writingCtx,
                    id: in_flight_block_id
                }).await;
            }
            println!("Doing spilling....");
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
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use bytes::Bytes;
    use log::info;
    use tokio::time;
    use crate::app::{PartitionedUId, ReadingOptions, ReadingViewContext, RequireBufferContext, WritingViewContext};
    use crate::config::{Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig, StorageType};
    use crate::store::hybrid::HybridStore;
    use crate::store::{PartitionedDataBlock, ResponseData, Store};
    use crate::store::ResponseData::mem;

    #[tokio::test]
    async fn test_insert_and_get() {
        env_logger::init();

        let data = b"hello world!";
        let data_len = data.len();

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let config = Config {
            memory_store: Some(MemoryStoreConfig {
                capacity: (data_len * 1) as i64
            }),
            localfile_store: Some(LocalfileStoreConfig {
                data_paths: vec![temp_path]
            }),
            hybrid_store: Some(HybridStoreConfig {
                memory_spill_high_watermark: 0.8,
                memory_spill_low_watermark: 0.2,
            }),
            store_type: Some(StorageType::MEMORY_LOCALFILE)
        };

        /// The hybrid store will flush the memory data to file when
        /// the data reaches the number of 4

        let mut store = Arc::new(HybridStore::from(config));
        // store.clone().start_flush_in_background();

        let uid = PartitionedUId {
            app_id: "1000".to_string(),
            shuffle_id: 0,
            partition_id: 0
        };

        for _ in vec![1, 2] {
            let writingCtx = WritingViewContext {
                uid: uid.clone(),
                data_blocks: vec![
                    PartitionedDataBlock {
                        block_id: 0,
                        length: data_len as i32,
                        uncompress_length: 100,
                        crc: 0,
                        data: Bytes::copy_from_slice(data),
                        task_attempt_id: 0
                    },
                    PartitionedDataBlock {
                        block_id: 0,
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

        time::sleep(Duration::from_secs(1)).await;

        let readingViewCtx = ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 24)
        };

        // let read_data = store.get(readingViewCtx).await;
        // if read_data.is_err() {
        //     panic!();
        // }
        //
        // match read_data.unwrap() {
        //     mem(mem_data) => {
        //         assert_eq!(Bytes::copy_from_slice(data), mem_data.data);
        //         let segments = mem_data.shuffle_data_block_segments;
        //         assert_eq!(1, segments.len());
        //     },
        //     _ => panic!()
        // }
    }
}
