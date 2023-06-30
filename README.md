Another implementation of Apache Uniffle shuffle server

## Build

`cargo build --release`


## Config

config.toml as follows:

``` 
store_type = "MEMORY_LOCALFILE"
grpc_port = 21100
coordinator_quorum = ["xxxxxxx1", "xxxxxxx2]
tags = ["datanode"]

[memory_store]
capacity = "100G"

[localfile_store]
data_paths = ["/data1/uniffle", "/data2/uniffle"]
healthy_check_min_disks = 0

[hdfs_store]
data_path = "hdfs://rbf-x/user/bi"
max_concurrency = 10

[hybrid_store]
memory_spill_high_watermark = 0.8
memory_spill_low_watermark = 0.2
memory_single_buffer_max_spill_size = "256M"

[metrics]
http_port = 19998
push_gateway_endpoint = "http://xxxxxxxxxxxxxx/pushgateway"
``` 

## Run

`DATANODE_IP={ip} RUST_LOG=info DATANODE_CONFIG_PATH=./config.toml ./datanode`

### HDFS Setup 

```shell
export JAVA_HOME=/path/to/java
export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/amd64/server:${LD_LIBRARY_PATH}

export HADOOP_HOME=/path/to/hadoop
export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)
``` 