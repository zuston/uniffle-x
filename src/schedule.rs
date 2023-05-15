use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use dashmap::DashMap;
use log::warn;
use thiserror::Error;
use crate::app::{PartitionedDataContext, ReadOptions};
use crate::proto::uniffle::{ShuffleBlock, ShuffleData};

#[derive(Debug)]
pub struct ClientData {
    pub partitionId: i32,
    pub blocks: Vec<ClientDataBlock>
}

#[derive(Debug)]
pub struct ClientDataBlock {
    pub block_id: i64,
    pub length: i32,
    pub uncompress_length: i32,
    pub crc: i64,
    pub data: Bytes,
    pub task_attempt_id: i64,
}

// impl ClientDataBlock {
//     fn new(block_id: i64, length: i32, uncompress_len: i32, crc: i64, data: Bytes, task_id: i64) -> ClientDataBlock {
//         ClientDataBlock {
//             block_id,
//             length,
//             uncompress_length,
//             crc,
//             data,
//             task_attempt_id
//         }
//     }
// }

impl From<ShuffleData> for ClientData {
    fn from(shuffleData: ShuffleData) -> ClientData {

        let mut blocks = vec![];
        for data in shuffleData.block {
            let block = ClientDataBlock {
                block_id: data.block_id,
                length: data.length,
                uncompress_length: data.uncompress_length,
                crc: data.crc,
                data: Bytes::from(data.data),
                task_attempt_id: data.task_attempt_id
            };
            blocks.push(block);
        }
        ClientData {
            partitionId: shuffleData.partition_id,
            blocks: blocks
        }
    }
}

enum ResponseDataIndex {
    local(LocalDataIndex)
}

pub struct LocalDataIndex {
    pub index_data: Bytes,
    pub data_file_len: i64,
}

enum ResponseData {
    local(LocalData),
    mem(MemoryData)
}

pub struct LocalData {
    pub data: Bytes,
}

pub struct MemoryData {
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


struct WriteEvent {
    ctx: PartitionedDataContext,
    data: ClientData,
    allocated: Option<bool>
}

#[derive(Debug)]
struct ReadEvent {
    ctx: PartitionedDataContext,
    readOptions: ReadOptions
}

struct ReadIndexEvent {
    ctx: PartitionedDataContext,
    readOptions: ReadOptions
}

// ===========================================

#[derive(Debug)]
struct StagingBuffer {
    size: i64,
    blocks: Vec<ClientDataBlock>
}

impl StagingBuffer {
    fn new() -> StagingBuffer {
        StagingBuffer {
            size: 0,
            blocks: vec![]
        }
    }
}

struct MemoryBudgt {
    capacity: i64,
    allocated: AtomicI64,
    used: AtomicI64
}

impl MemoryBudgt {
    fn new(capacity: i64) -> MemoryBudgt {
        MemoryBudgt {
            capacity,
            allocated: AtomicI64::new(0),
            used: AtomicI64::new(0)
        }
    }
}

// ===========================================


trait Store {
    fn put(&mut self, event: WriteEvent) -> Result<bool>;
    fn get(&mut self, event: ReadEvent) -> Result<ResponseData>;
    fn get_index(&mut self, event: ReadIndexEvent) -> Result<ResponseDataIndex>;
    fn require_buffer(&mut self, required_size: i64) -> Result<(i32, i32)>;
    fn next(&mut self) -> Arc<Vec<Box<dyn Store>>>;
    fn clear(&mut self, appId: String) -> Result<bool>;
}

struct MemoryStore {
    memBudget: MemoryBudgt,
    table: DashMap<PartitionedDataContext, StagingBuffer>,
    in_flight_data: DashMap<PartitionedDataContext, StagingBuffer>,
    max_size_per_buffer: Option<i64>,
    write_lock: Mutex<()>,
}

impl MemoryStore {
    fn new(capacity: i64) -> MemoryStore {
        let mem_budgt = MemoryBudgt::new(capacity);
        MemoryStore {
            memBudget: mem_budgt,
            table: DashMap::new(),
            in_flight_data: DashMap::new(),
            max_size_per_buffer: None,
            write_lock: Mutex::new(())
        }
    }
}

impl Store for MemoryStore {
    fn put(&mut self, event: WriteEvent) -> Result<bool> {
        let _lock = self.write_lock.lock().unwrap();

        let blocks = event.data.blocks;

        let mut buffer = self.table.entry(event.ctx).or_insert_with(|| StagingBuffer::new());
        let mut len = 0i64;
        for block in blocks {
            len += block.length as i64;
            buffer.blocks.push(block);
        }

        match Some(self.max_size_per_buffer) {
            None => {}
            Some(_) => {}
        }

        let  budget = &self.memBudget;
        budget.used.fetch_add(len, Ordering::SeqCst);
        budget.allocated.fetch_sub(len, Ordering::SeqCst);

        Ok(true)
    }

    fn get(&mut self, event: ReadEvent) -> Result<ResponseData> {
        warn!("Errors on getting empty data from memory. It should not happen. event: {:?}", event);
        Err(anyhow::anyhow!(""))
    }

    fn get_index(&mut self, event: ReadIndexEvent) -> Result<ResponseDataIndex> {
        todo!()
    }

    fn require_buffer(&mut self, required_size: i64) -> Result<(i32, i32)> {
        todo!()
    }

    fn next(&mut self) -> Arc<Vec<Box<dyn Store>>> {
        todo!()
    }

    fn clear(&mut self, appId: String) -> Result<bool> {
        todo!()
    }
}

