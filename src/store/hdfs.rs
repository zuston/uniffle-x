use std::{env, io};
use std::path::Path;
use std::sync::Arc;
use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingViewContext, RequireBufferContext, WritingViewContext};
use crate::store::{Persistent, ResponseData, ResponseDataIndex, Store};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use futures::AsyncWriteExt;
use hdrs::{Client};
use log::{error, info};
use tokio::sync::Mutex;
use tracing::debug;
use crate::config::HdfsStoreConfig;
use url::{Url, ParseError};
use url::form_urlencoded::parse;
use anyhow::Result;
use async_channel::Sender;
use futures::future::select;

pub struct HdfsStore {
    root: String,
    filesystem: Box<Hdrs>,
    partition_file_locks: DashMap<String, Arc<Mutex<()>>>
}

unsafe impl Send for HdfsStore {}
unsafe impl Sync for HdfsStore {}
impl Persistent for HdfsStore {}

impl HdfsStore {
    pub fn from(conf: HdfsStoreConfig) -> Self {
        let data_path = conf.data_path;
        let data_url = Url::parse(data_path.as_str()).unwrap();

        let name_node = match data_url.host_str() {
            Some(host) => format!("{}://{}", data_url.scheme(), host),
            _ => "default".to_string()
        };
        let krb5_cache = env::var("KRB5CACHE_PATH").map_or(None, |v| Some(v));
        let hdfs_user = env::var("HDFS_USER").map_or(None, |v| Some(v));

        let fs = Hdrs::new(name_node.as_str(), krb5_cache, hdfs_user);
        if fs.is_err() {
            error!("Errors on connecting the hdfs. error: {:?}", fs.err());
            panic!();
        }
        let filesystem = fs.unwrap();

        HdfsStore {
            root: data_url.to_string(),
            filesystem: Box::new(filesystem),
            partition_file_locks: DashMap::new()
        }
    }

    pub(crate) fn get_file_path_by_uid(&self, uid: &PartitionedUId) -> (String, String) {
        let app_id = &uid.app_id;
        let shuffle_id = &uid.shuffle_id;
        let p_id = &uid.partition_id;

        (
            format!("{}/{}/{}/{}-{}/partition-{}.data", &self.root, app_id, shuffle_id, p_id, p_id, p_id),
            format!("{}/{}/{}/{}-{}/partition-{}.index", &self.root, app_id, shuffle_id, p_id, p_id, p_id)
        )
    }
}

#[async_trait]
impl Store for HdfsStore {
    fn start(self: Arc<Self>) {
        info!("There is nothing to do in hdfs store");
    }

    async fn insert(&self, ctx: WritingViewContext) -> anyhow::Result<()> {
        let uid = ctx.uid;
        let data_blocks = ctx.data_blocks;

        let (data_file_path, index_file_path) = self.get_file_path_by_uid(&uid);

        let lock_cloned = self.partition_file_locks.entry(data_file_path.clone()).or_insert_with(|| Arc::new(Mutex::new(()))).clone();
        let lock_guard = lock_cloned.lock().await;

        // todo: optimize creating once.
        let parent_dir = Path::new(data_file_path.as_str()).parent().unwrap();
        let parent_path_str = format!("{}/", parent_dir.to_str().unwrap());
        debug!("creating dir: {}", parent_path_str.as_str());
        self.filesystem.create_dir(parent_path_str.as_str()).await?;

        let mut index_bytes_holder = BytesMut::new();
        let mut data_bytes_holder = BytesMut::new();

        let mut next_offset = self.filesystem.len(&data_file_path).await.map_or(0, |v| v as i64);

        for data_block in data_blocks {
            let block_id = data_block.block_id;
            let crc = data_block.crc;
            let length = data_block.length;
            let task_attempt_id = data_block.task_attempt_id;
            let uncompress_len = data_block.uncompress_length;

            index_bytes_holder.put_i64(next_offset);
            index_bytes_holder.put_i32(length);
            index_bytes_holder.put_i32(uncompress_len);
            index_bytes_holder.put_i64(crc);
            index_bytes_holder.put_i64(block_id);
            index_bytes_holder.put_i64(task_attempt_id);

            let data = data_block.data;
            data_bytes_holder.extend_from_slice(&data);

            next_offset += length as i64;
        }

        self.filesystem.append(&data_file_path, data_bytes_holder.freeze()).await?;
        self.filesystem.append(&index_file_path, index_bytes_holder.freeze()).await?;

        Ok(())
    }

    async fn get(&self, ctx: ReadingViewContext) -> anyhow::Result<ResponseData> {
        todo!()
    }

    async fn get_index(&self, ctx: ReadingIndexViewContext) -> anyhow::Result<ResponseDataIndex> {
        todo!()
    }

    async fn require_buffer(&self, ctx: RequireBufferContext) -> anyhow::Result<(bool, i64)> {
        todo!()
    }

    async fn purge(&self, app_id: String) -> anyhow::Result<()> {
        info!("Have not supported");
        Ok(())
    }

    async fn is_healthy(&self) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[async_trait]
trait HdfsDelegator {
    async fn append(&self, file_path: &str, data: Bytes) -> Result<()>;
    async fn create_dir(&self, dir: &str) -> Result<()>;
    async fn len(&self, file_path: &str) -> Result<u64>;
}

struct Hdrs {
    client: Client
}

#[async_trait]
impl HdfsDelegator for Hdrs {

    async fn append(&self, file_path: &str, data: Bytes) -> Result<()> {
        // check the file existence, if not, create it.
        // todo: put into the cache.
        let metadata = self.client.metadata(file_path);
        if metadata.is_err() && metadata.unwrap_err().kind() == io::ErrorKind::NotFound {
            debug!("Creating the file, path: {}", file_path);
            let mut write = self.client.open_file().create(true).write(true).async_open(file_path).await?;
            write.write("".as_bytes()).await?;
            write.flush().await?;
            write.close().await?;
            debug!("the file: {} is created!", file_path);
        }

        let mut data_writer = self.client
            .open_file()
            .create(true)
            .append(true)
            .async_open(file_path)
            .await?;
        data_writer.write_all(data.as_ref()).await?;
        data_writer.flush().await?;
        data_writer.close().await?;
        debug!("data has been flushed. path: {}", file_path);

        Ok(())
    }

    async fn create_dir(&self, dir: &str) -> Result<()> {
        self.client.create_dir(dir)?;
        Ok(())
    }

    async fn len(&self, file_path: &str) -> Result<u64> {
        let meta = self.client.metadata(file_path)?;
        Ok(
            meta.len()
        )
    }
}

impl Hdrs {
    fn new(name_node: &str, krb5_cache: Option<String>, user: Option<String>) -> Result<Self> {
        let client = Client::connect(name_node, user.unwrap().as_str(), krb5_cache.unwrap().as_str())?;
        Ok(
            Hdrs {
                client
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn dir_test() -> anyhow::Result<()> {
        let file_path = "app/0/1.data";
        let parent_path = Path::new(file_path).parent().unwrap();
        println!("{}", parent_path.to_str().unwrap());

        Ok(())
    }
}