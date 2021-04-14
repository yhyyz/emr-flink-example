### 更详细的程序及使用说明，会随代码发给客户
### 程序包含三个实现
1. Kafka2S3Text
2. kafka2S3Parquet
3. Kafka2S3Hive


### Kafka 中数据样例
Kafka中是JSON数据，样例数据如下，测试时可以用以下样例格式发送数据到Kafka
```
{"uuid":"999d0f4f-9d49-4ad0-9826-7a01600ed0b8","date":"2021-04-13T06:23:10.593Z","timestamp":1617171790593,"ad_type":1203,"ad_type_name":"udxyt"}
```
### Kafka2S3Text
Flink 消费Kafka数据写到S3，以文本方式自定义分区方式存储。

### Kafka2S3Parquet
Flink 消费Kafka数据写到S3，Parquet存储，自定义分区。

### Kafka2S3Parquet
Flink 消费Kafka数据写到Hive S3 外部表，分区信息Hive Metastore存储


