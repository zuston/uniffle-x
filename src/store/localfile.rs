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
use crate::store::{ResponseData, ResponseDataIndex, Store};
use async_trait::async_trait;
use tokio::sync::RwLock;

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
        let local_disk = self.partition_written_disk_map.entry(uid).or_insert_with(||{
            // todo: support hash selection or more pluggable selections
            self.local_disks.get(0).unwrap().clone()
        }).clone();

        self.file_locks.entry(data_file_path.clone()).or_insert_with(|| RwLock::new(())).write().await;

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
        match local_disk.write(bytes_holder.freeze(), data_file_path.clone()).await {
            Ok(_) => {
                local_disk.write(index_bytes_holder.freeze(), index_file_path).await
            },
            _ => Ok(())
        }
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

#[cfg(test)]
mod test {
    use std::io::Read;
    use bytes::Bytes;
    use crate::store::localfile::LocalDisk;

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
}