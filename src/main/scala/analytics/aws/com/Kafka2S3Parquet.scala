package analytics.aws.com

import analytics.aws.com.conf.Config
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.{SimpleStringSchema}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.util.Properties

object Kafka2S3Parquet {

  case class Data(uuid: String, date: String, timestamp: Long, ad_type: Int, ad_type_name: String)

  // 自定义Bucket，按照原始日志的date字段值分区, kafka 中原始日志样例
  // {"uuid":"999d0f4f-9d49-4ad0-9826-7a01600ed0b8","date":"2021-04-13T06:23:10.593Z","timestamp":1617171790593,"ad_type":1203,"ad_type_name":"udxyt"}
  class CustomBucketAssigner extends BasePathBucketAssigner[Data] {
    override def getBucketId(element: Data, context: BucketAssigner.Context): String = s"logday=${
      val tmp = element.date.split("T")
      if (tmp.nonEmpty) {
        tmp(0).replaceAll("-", "")
      } else {
        "error"
      }
    }"
  }

  def createKafkaSource(env: StreamExecutionEnvironment, params: Config): DataStream[String] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", params.brokerList)
    properties.setProperty("group.id", params.groupId)
    val myConsumer = new FlinkKafkaConsumer[String](params.sourceTopic, new SimpleStringSchema(), properties) // start from the earliest record possiblemyConsumer.setStartFromLatest()        // start from the latest record
    env.addSource(myConsumer)
  }

  def createParquetSink(params: Config): StreamingFileSink[Data] = {
    val sink = StreamingFileSink
      // bulk format 只支持checkpoint的滚动策略，每次checkpoint滚动文件
      .forBulkFormat(
        new Path(params.output),
        // case class 反射获取schema
        ParquetAvroWriters.forReflectRecord(classOf[Data])
      ).withBucketAssigner(new CustomBucketAssigner)
      .build()
    sink
  }

  def main(args: Array[String]) {
    val parmas = Config.parseConfig(Kafka2S3Parquet, args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(parmas.checkpointInterval.toInt * 1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val rocksBackend: StateBackend = new RocksDBStateBackend(parmas.checkpointDir)
    env.setStateBackend(rocksBackend)
    // create kafka source
    val source = createKafkaSource(env, parmas)
    // kafka text data sink to s3
    source.map(line => {
      val event: Data = JSON.parseObject(line, classOf[Data])
      event
    }).addSink(createParquetSink(parmas))
    env.execute("stream parquet to s3")
  }

}
