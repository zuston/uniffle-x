pub mod memory;
pub mod localfile;
pub mod hybrid;

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
use tokio::{fs, select};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tonic::codegen::ok;
use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingViewContext, RequireBufferContext, WritingViewContext};
use crate::app::ReadingOptions::{MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE};
use crate::proto::uniffle::{ShuffleBlock, ShuffleData, ShuffleDataBlockSegment};
use crate::store::ResponseDataIndex::local;
use async_trait::async_trait;
use crate::config::Config;
use crate::store::hybrid::HybridStore;
use crate::store::memory::MemoryStore;

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

impl ResponseData {
    pub fn from_local(&self) -> Bytes {
        match self {
            ResponseData::local(data) => data.data.clone(),
            _ => Default::default()
        }
    }

    pub fn from_memory(&self) -> PartitionedMemoryData {
        match self {
            ResponseData::mem(data) => data.clone(),
            _ => Default::default()
        }
    }
}

pub struct PartitionedLocalData {
    pub data: Bytes,
}

#[derive(Clone, Default)]
pub struct PartitionedMemoryData {
    pub shuffle_data_block_segments: Vec<DataSegment>,
    pub data: Bytes
}

#[derive(Clone)]
pub struct DataSegment {
    pub block_id: i64,
    pub offset: i64,
    pub length: i32,
    pub uncompress_length: i32,
    pub crc: i64,
    pub task_attempt_id: i64,
}

impl Into<ShuffleDataBlockSegment> for DataSegment {
    fn into(self) -> ShuffleDataBlockSegment {
        ShuffleDataBlockSegment {
            block_id: self.block_id,
            offset: self.offset,
            length: self.length,
            uncompress_length: self.uncompress_length,
            crc: self.crc,
            task_attempt_id: self.task_attempt_id
        }
    }
}

// =====================================================

#[async_trait]
pub trait Store {
    fn start(self: Arc<Self>);
    async fn insert(&self, ctx: WritingViewContext) -> Result<()>;
    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData>;
    async fn get_index(&self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex>;
    async fn require_buffer(&self, ctx: RequireBufferContext) -> Result<(bool, i64)>;
    async fn purge(&self, app_id: String) -> Result<()>;
}

pub struct StoreProvider {}

impl StoreProvider {
    pub fn get(config: Config) -> HybridStore {
        HybridStore::from(config)
    }
}
