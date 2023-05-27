use std::borrow::Borrow;
use std::cell::Ref;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::{Arc};
use dashmap::DashMap;
use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingViewContext, RequireBufferContext, WritingViewContext};
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use crate::store::{LocalDataIndex, PartitionedLocalData, ResponseData, ResponseDataIndex, Store};
use async_trait::async_trait;
use tokio::sync::RwLock;
use tonic::codegen::ok;
use crate::app::ReadingOptions::FILE_OFFSET_AND_LEN;
use crate::config::LocalfileStoreConfig;

fn create_directory_if_not_exists(dir_path: &str) {
    if !std::fs::metadata(dir_path).is_ok() {
        std::fs::create_dir_all(dir_path).expect("Errors on creating dirs.");
    }
}

pub struct LocalFileStore {
    local_disks: Vec<Arc<LocalDisk>>,
    partition_written_disk_map: DashMap<PartitionedUId, Arc<LocalDisk>>,
    file_locks: DashMap<String, RwLock<()>>,
}

unsafe impl Send for LocalFileStore {}
unsafe impl Sync for LocalFileStore {}

impl LocalFileStore {
    pub fn new(local_disks: Vec<String>) -> Self {
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

    pub fn from(localfile_config: LocalfileStoreConfig) -> Self {
        let mut local_disk_instances = vec![];
        for path in localfile_config.data_paths {
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
    fn start(self: Arc<Self>) {
        todo!()
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<()> {
        let uid = ctx.uid;
        let (data_file_path, index_file_path) = LocalFileStore::gen_relative_path(&uid);
        let local_disk = self.partition_written_disk_map.entry(uid).or_insert_with(||{
            // todo: support hash selection or more pluggable selections
            self.local_disks.get(0).unwrap().clone()
        }).clone();

        self.file_locks.entry(data_file_path.clone()).or_insert_with(|| RwLock::new(())).write().await?;

        // write index file and data file
        // todo: split multiple pieces
        let mut bytes_holder = BytesMut::new();
        let mut next_offset = local_disk.next_offset(&data_file_path);

        let mut index_bytes_holder = BytesMut::new();
        for block in ctx.data_blocks {

            let block_id = block.block_id;
            let length = block.length;
            let uncompress_len = block.uncompress_length;
            let task_attempt_id = block.task_attempt_id;
            let crc = block.crc;

            index_bytes_holder.put_i64(next_offset);
            index_bytes_holder.put_i32(length);
            index_bytes_holder.put_i32(uncompress_len);
            index_bytes_holder.put_i64(crc);
            index_bytes_holder.put_i64(block_id);
            index_bytes_holder.put_i64(task_attempt_id);

            let data = block.data;
            bytes_holder.extend_from_slice(&*data);
            next_offset += length as i64;
        }

        // data file write
        local_disk.write(bytes_holder.freeze(), data_file_path.clone()).await?;
        local_disk.write(index_bytes_holder.freeze(), index_file_path).await?;
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData> {
        let uid = ctx.uid;
        let (offset, len) = match ctx.reading_options {
            FILE_OFFSET_AND_LEN(offset, len) => (offset, len),
            _ => (0, 0)
        };

        if len == 0 {
            return Ok(ResponseData::local(PartitionedLocalData {
                data: Default::default()
            }))
        }

        let (data_file_path, _) = LocalFileStore::gen_relative_path(&uid);
        let _ = self.file_locks.entry(data_file_path.clone()).or_insert_with(|| RwLock::new(())).read().await;

        let local_disk: Option<Arc<LocalDisk>> = self.partition_written_disk_map.get(&uid).map(|v|v.value().clone());

        if local_disk.is_none() {
            return Ok(ResponseData::local(PartitionedLocalData {
                data: Default::default()
            }))
        }

        let local_disk = local_disk.unwrap();
        let data = local_disk.read(data_file_path, offset, Some(len)).await;
        Ok(ResponseData::local(PartitionedLocalData {
            data: data.unwrap()
        }))
    }

    async fn get_index(&self, ctx: ReadingIndexViewContext) -> Result<ResponseDataIndex> {
        let uid = ctx.partition_id;
        let (data_file_path, index_file_path) = LocalFileStore::gen_relative_path(&uid);
        let _ = self.file_locks.entry(data_file_path.clone()).or_insert_with(|| RwLock::new(())).read().await;

        let local_disk: Option<Arc<LocalDisk>> = self.partition_written_disk_map.get(&uid).map(|v|v.value().clone());

        if local_disk.is_none() {
            return Ok(ResponseDataIndex::local(LocalDataIndex {
                index_data: Default::default(),
                data_file_len: 0
            }));
        }

        let index_data_result = local_disk.unwrap().read(index_file_path, 0, None).await.unwrap();
        let len = index_data_result.clone().len() as i64;
        Ok(ResponseDataIndex::local(LocalDataIndex {
            index_data: index_data_result,
            data_file_len: len
        }))
    }

    async fn require_buffer(&self, ctx: RequireBufferContext) -> Result<(bool, i64)> {
        panic!("It should happen")
    }

    async fn purge(&self, app_id: String) -> Result<()> {
        todo!()
    }
}

struct LocalDisk {
    base_path: String,
    partition_file_len: DashMap<String, i64>
}

impl LocalDisk {
    fn new(path: String) -> Self {
        create_directory_if_not_exists(&path);
        LocalDisk {
            base_path: path,
            partition_file_len: DashMap::new()
        }
    }

    fn append_path(&self, path: String) -> String {
        format!("{}/{}", self.base_path.clone(), path)
    }

    fn next_offset(&self, data_file_name: &str) -> i64 {
        match self.partition_file_len.get(data_file_name).map(|v|v.value().clone()) {
            Some(offset) => offset,
            _ => 0
        }
    }

    async fn write(&self, data: Bytes, relative_file_path: String) -> Result<()> {
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

        println!("data file: {}", absolute_path.clone());

        let mut output_file= OpenOptions::new().append(true).create(true).open(absolute_path).await?;
        output_file.write_all(data.as_ref()).await?;
        output_file.sync_data().await?;

        let mut val = self.partition_file_len.entry(relative_file_path).or_insert(0);
        *val += data.len() as i64;

        Ok(())
    }

    async fn read(&self, relative_file_path: String, offset: i64, length: Option<i64>) -> Result<Bytes> {
        let file_path = self.append_path(relative_file_path);

        let file = tokio::fs::File::open(&file_path).await?;

        let read_len = match length {
            Some(len) => len,
            _ => file.metadata().await?.len().try_into().unwrap()
        } as usize;

        let mut reader = tokio::io::BufReader::new(file);
        let mut buffer = vec![0; read_len];
        reader.seek(SeekFrom::Start(offset as u64)).await?;
        reader.read_exact(buffer.as_mut()).await?;

        let mut bytes_buffer = BytesMut::new();
        bytes_buffer.extend_from_slice(&*buffer);
        Ok(bytes_buffer.freeze())
    }
}

#[cfg(test)]
mod test {
    use std::fs::read;
    use std::io::Read;
    use bytes::{Buf, Bytes, BytesMut};
    use log::info;
    use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingOptions, ReadingViewContext, WritingViewContext};
    use crate::store::localfile::{LocalDisk, LocalFileStore};
    use crate::store::{PartitionedDataBlock, ResponseData, ResponseDataIndex, Store};

    #[tokio::test]
    async fn local_store_test() {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("init local file path: {}", temp_path);
        let mut local_store = LocalFileStore::new(vec![temp_path]);

        let uid = PartitionedUId {
            app_id: "100".to_string(),
            shuffle_id: 0,
            partition_id: 0
        };

        let data = b"hello world!hello china!";
        let size = data.len();
        let writingCtx = WritingViewContext {
            uid: uid.clone(),
            data_blocks: vec![
                PartitionedDataBlock {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0
                },
                PartitionedDataBlock {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0
                }
            ]
        };

        let insert_result = local_store.insert(writingCtx).await;
        if insert_result.is_err() {
            panic!()
        }

        async fn get_and_check_partitial_data(local_store: &mut LocalFileStore, uid: PartitionedUId, size: i64, expected: &[u8]) {
            let readingCtx = ReadingViewContext {
                uid,
                reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(0, size as i64)
            };

            let read_result = local_store.get(readingCtx).await;
            if read_result.is_err() {
                panic!()
            }

            match read_result.unwrap() {
                ResponseData::local(partitioned_data) => {
                    assert_eq!(expected, partitioned_data.data.as_ref());
                },
                _ => panic!()
            }
        }

        // case1: read the one partition block data
        get_and_check_partitial_data(&mut local_store, uid.clone(), size as i64, data).await;

        // case2: read the complete block data
        let mut expected = BytesMut::with_capacity(size* 2);
        expected.extend_from_slice(data);
        expected.extend_from_slice(data);
        get_and_check_partitial_data(&mut local_store, uid.clone(), size as i64 * 2, expected.freeze().as_ref()).await;

        // case3: get the index data
        let readingIndexViewCtx = ReadingIndexViewContext {
            partition_id: uid.clone()
        };
        let result = local_store.get_index(readingIndexViewCtx).await;
        if result.is_err() {
            panic!()
        }

        match result.unwrap() {
            ResponseDataIndex::local(data) => {
                let mut index = data.index_data;
                let offset_1 = index.get_i64();
                assert_eq!(0, offset_1);
                let length_1 = index.get_i32();
                assert_eq!(size as i32, length_1);
                index.get_i32();
                index.get_i64();
                let block_id_1 = index.get_i64();
                assert_eq!(0, block_id_1);
                let task_id = index.get_i64();
                assert_eq!(0, task_id);

                let offset_2 = index.get_i64();
                assert_eq!(size as i64, offset_2);
                assert_eq!(size as i32, index.get_i32());
            },
            _ => panic!()
        }

        temp_dir.close().unwrap();
    }

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

        let read_result = local_disk.read(relative_path.to_string(), 0, Some(data.len() as i64 * 2)).await;
        assert!(read_result.is_ok());
        let read_data = read_result.unwrap();
        let expected = b"Hello, World!Hello, World!";
        assert_eq!(read_data.as_ref(), expected);

        temp_dir.close().unwrap();
    }
}