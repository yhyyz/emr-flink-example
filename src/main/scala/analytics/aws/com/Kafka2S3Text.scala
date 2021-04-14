package analytics.aws.com

import analytics.aws.com.conf.Config
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.TimeUnit

object Kafka2S3Text {

  // kafka 中样例数据如下：
  // {"uuid":"999d0f4f-9d49-4ad0-9826-7a01600ed0b8","date":"2021-04-13T06:23:10.593Z","timestamp":1617171790593,"ad_type":1203,"ad_type_name":"udxyt"}
  def createKafkaSource(env: StreamExecutionEnvironment,parmas:Config):DataStream[String]={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", parmas.brokerList)
    properties.setProperty("group.id", parmas.groupId)
    val myConsumer = new FlinkKafkaConsumer[String](parmas.sourceTopic, new SimpleStringSchema(), properties) // start from the earliest record possiblemyConsumer.setStartFromLatest()        // start from the latest record
    env.addSource(myConsumer)
  }


  def createFileSink(params:Config) = {
    val sink = StreamingFileSink
      .forRowFormat(new Path(params.output), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          // 文件滚动策略，滚动时间，文件不活跃时间，文件大小，三个条件控制，任何一个达到就触发滚动
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(Integer.parseInt(params.rolloverInterval)))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(Integer.parseInt(params.inactivityInterval)))
          .withMaxPartSize(Integer.parseInt(params.maxPartSize))
          .build())
          .withBucketAssigner(new CustomBucketAssigner)
      .build()
    sink
  }

  // 以processing time作为分区目录
  class CustomBucketAssigner extends BasePathBucketAssigner[String] {
    override def getBucketId(element: String, context: BucketAssigner.Context): String = s"logday=${
      val formatterLogday: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      val dateTime = LocalDateTime.ofEpochSecond(context.currentProcessingTime() / 1000L, 0, ZoneOffset.ofHours(8))
      formatterLogday.format(dateTime)
    }"
  }

  def main(args: Array[String]) {
    val parmas = Config.parseConfig(Kafka2S3Text, args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(parmas.checkpointInterval.toInt*1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val  rocksBackend:StateBackend =new RocksDBStateBackend(parmas.checkpointDir)
    env.setStateBackend(rocksBackend)
    // create kafka source
    val source = createKafkaSource(env,parmas)
    // kafka text data sink to s3
    source.addSink(createFileSink(parmas))
    env.execute("text stream to s3")
  }

}
