use std::sync::Arc;
use crate::app::{ReadingIndexViewContext, ReadingOptions, ReadingViewContext, RequireBufferContext, WritingViewContext};
use crate::store::memory::MemoryStore;
use crate::store::{ResponseData, ResponseDataIndex, Store};
use crate::store::localfile::LocalFileStore;
use async_trait::async_trait;
use anyhow::{Result, anyhow};
use crate::config;
use crate::config::{Config, HybridStoreConfig};

struct HybridStore {
    // Box<dyn Store> will build fail
    hot_store: Box<dyn Store + Send + Sync>,
    warm_store: Box<dyn Store + Send + Sync>,
    config: HybridStoreConfig,
}

unsafe impl Send for HybridStore {}
unsafe impl Sync for HybridStore {}

impl HybridStore {
    fn from(config: Config) -> Self {
        HybridStore {
            hot_store: Box::new(MemoryStore::from(config.memory_store.unwrap())),
            warm_store: Box::new(LocalFileStore::from(config.localfile_store.unwrap())),
            config: config.hybrid_store.unwrap()
        }
    }
}

#[async_trait]
impl Store for HybridStore {
    async fn insert(&mut self, ctx: WritingViewContext) -> Result<()> {
        // whether the memory reach the high watermark
        Ok(())
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
