use std::env;
use std::sync::Arc;
use crate::app::{PartitionedUId, ReadingIndexViewContext, ReadingViewContext, RequireBufferContext, WritingViewContext};
use crate::store::{Persistent, ResponseData, ResponseDataIndex, Store};
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use log::info;
use opendal::{Entry, Metakey, Operator};
use opendal::services::Hdfs;
use crate::config::HdfsStoreConfig;

pub struct HdfsStore {
    basic_path: String,
    operator: Operator,
}

impl HdfsStore {
    pub fn from(conf: HdfsStoreConfig) -> Self {
        let basic_path = conf.data_path;

        let mut builder = Hdfs::default();
        builder.name_node("default");
        builder.root(basic_path.as_str());

        let krb5_cache = env::var("KRB5CACHE_PATH");
        if krb5_cache.is_ok() {
            builder.kerberos_ticket_cache_path(&krb5_cache.unwrap());
        }

        let op: Operator = Operator::new(builder).expect("Errors on initializing opendal hdfs operator").finish();
        
        HdfsStore {
            basic_path,
            operator: op
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