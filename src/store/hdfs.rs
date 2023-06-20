use std::env;
use std::path::Path;
use std::sync::Arc;
use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingViewContext, RequireBufferContext, WritingViewContext};
use crate::store::{Persistent, ResponseData, ResponseDataIndex, Store};
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use log::info;
use opendal::{Entry, Metakey, Operator};
use opendal::services::Hdfs;
use tokio::sync::Mutex;
use crate::config::HdfsStoreConfig;
use url::{Url, ParseError};
use url::form_urlencoded::parse;

pub struct HdfsStore {
    basic_path: String,
    operator: Operator,
    partition_file_locks: DashMap<String, Arc<Mutex<()>>>
}

impl HdfsStore {
    pub fn from(conf: HdfsStoreConfig) -> Self {
        let data_path = conf.data_path;
        let data_url = Url::parse(data_path.as_str()).unwrap();

        let name_node = match data_url.host_str() {
            Some(host) => format!("{}://{}", data_url.scheme(), host),
            _ => "default".to_string()
        };

        let mut builder = Hdfs::default();
        builder.name_node(&name_node);

        builder.root(data_url.path());

        let krb5_cache = env::var("KRB5CACHE_PATH");
        if krb5_cache.is_ok() {
            builder.kerberos_ticket_cache_path(&krb5_cache.unwrap());
        }

        let op: Operator = Operator::new(builder).expect("Errors on initializing opendal hdfs operator").finish();
        
        HdfsStore {
            basic_path: data_url.to_string(),
            operator: op,
            partition_file_locks: DashMap::new()
        }
    }

    pub(crate) fn get_file_path_by_uid(&self, uid: &PartitionedUId) -> (String, String) {
        let app_id = &uid.app_id;
        let shuffle_id = &uid.shuffle_id;
        let p_id = &uid.partition_id;

        (
            format!("{}/{}/partition-{}.data", app_id, shuffle_id, p_id),
            format!("{}/{}/partition-{}.index", app_id, shuffle_id, p_id)
        )
    }
}

impl Persistent for HdfsStore {}

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
        self.operator.create_dir(parent_path_str.as_str()).await?;

        let mut index_bytes_holder = BytesMut::new();
        let mut data_bytes_holder = BytesMut::new();

        let mut next_offset = {
            let metadata = self.operator.metadata(&Entry::new(&data_file_path), Metakey::ContentLength).await?;
            metadata.content_length() as i64
        };

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

        self.operator.append(&data_file_path, data_bytes_holder.freeze()).await?;
        self.operator.append(&index_file_path, index_bytes_holder.freeze()).await?;

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