Another implementation of uniffle shuffle server

### todo
1. support more metrics about different stores/writing failed and so on
2. support multiple disk for localfile
3. support health check for different stores.
4. support retry flushing to localfile when meeting timeout
5. support huge partition check and writing limit
6. support concurrency limit of writing for per disk
7. support s3
8. support hdfs
9. support single buffer flush