use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc};
use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext, RequireBufferContext, WritingViewContext};
use crate::store::memory::{MemoryStore, StagingBuffer};
use crate::store::{ResponseData, ResponseDataIndex, Store};
use crate::store::localfile::LocalFileStore;
use async_trait::async_trait;
use anyhow::{Result, anyhow};
use log::info;
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
    memory_spill_recv: mpsc::Receiver<SpillMessage>,
    memory_spill_send: mpsc::Sender<SpillMessage>
}

struct SpillMessage {
    ctx: WritingViewContext,
    id: i64
}

unsafe impl Send for HybridStore {}
unsafe impl Sync for HybridStore {}

impl HybridStore {
    fn from(config: Config) -> Self {
        let (send, recv) = mpsc::channel(100);
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

    fn background_actor_handle(store: Arc<Mutex<HybridStore>>) {
        let mut store = store.clone();
        tokio::spawn(async move {
            while let Some(message) = store.lock().await.memory_spill_recv.recv().await {
                let info = store.lock().await.memory_spill_to_localfile(message.ctx, message.id).await.expect("TODO: panic message");
                info!("{}", info);
            }
        });
    }

    async fn memory_spill_to_localfile(&mut self, ctx: WritingViewContext, in_flight_blocks_id: i64) -> Result<String> {
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
    async fn insert(&mut self, ctx: WritingViewContext) -> Result<()> {
        let spill_lock = self.memory_spill_lock.lock().await;
        let used_ratio = self.hot_store.memory_usage_ratio().await;
        if used_ratio > self.config.memory_spill_high_watermark {
            let target_size = 100;
            let buffers = self.hot_store.get_required_spill_buffer(target_size).await;

            for (partition_id, buffer) in buffers {
                let mut buffer_inner = buffer.lock().await;
                let blocks = buffer_inner.to_owned().blocks;
                let writingCtx = WritingViewContext {
                    uid: partition_id,
                    data_blocks: blocks.clone()
                };
                let in_flight_block_id = buffer_inner.add_blocks_to_send(blocks).unwrap();
                buffer_inner.blocks.clear();
                self.memory_spill_send.send(SpillMessage {
                    ctx: writingCtx,
                    id: in_flight_block_id
                });
            }
        }
        drop(spill_lock);

        self.hot_store.insert(ctx).await
    }

    async fn get(&mut self, ctx: ReadingViewContext) -> Result<ResponseData> {
        match ctx.reading_options {
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(_, _) => self.hot_store.get(ctx).await,
            _ => self.warm_store.get(ctx).await
        }
    }

    async fn get_index(&mut self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex> {
        self.warm_store.get_index(ctx).await
    }

    async fn require_buffer(&mut self, ctx: RequireBufferContext) -> Result<(bool, i64)> {
        self.hot_store.require_buffer(ctx).await
    }

    async fn purge(&mut self, app_id: String) -> Result<()> {
        Ok(())
    }
}
