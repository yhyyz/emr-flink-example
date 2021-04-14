package analytics.aws.com

import analytics.aws.com.conf.Config
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.hadoop.hive.conf.{HiveConf, HiveConfUtil}
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.TimeCharacteristic

object Kafka2S3Hive {
  val LOG: Logger = LoggerFactory.getLogger(Kafka2S3Hive.getClass)

  def main(args: Array[String]): Unit = {
    val params = Config.parseConfig(Kafka2S3Hive, args)
    LOG.info("start run flink kafka2hive ....")

    // 创建Flink StreamEnv ，设置rockdb state backend
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.enableCheckpointing(params.checkpointInterval.toInt * 1000)
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    streamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    streamEnv.getCheckpointConfig.setCheckpointTimeout(60000)
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val rocksBackend: StateBackend = new RocksDBStateBackend(params.checkpointDir)
    streamEnv.setStateBackend(rocksBackend)
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建 table env，流模式
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(streamEnv, settings)
    // 也可以直接创建table env 配置相关参数
    //    val tEnv = TableEnvironment.create(settings)
    //    tEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE)
    //    tEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20))

    // 设置Hive Catalog
    val name = "log-hive"
    val defaultDatabase = params.database
    // 可以通过读取resource目录下的hive配置文件

    // 配置HiveConf, flink 1.12 支持通过HiveConf设置相关参数创建HiveCatalog对象，但是Flink 1.11 只能通过hiveConfDir配置
    //      val  hiveConf=new HiveConf()
    //      hiveConf.set("hive.metastore.uris",params.metastore)
    val hive = new HiveCatalog(name, defaultDatabase, params.hiveConfDir, "3.1.2")
    tEnv.registerCatalog(name, hive)
    tEnv.useCatalog(name)

    // 创建Flink Kafka 表SQL， 从timestamp生成event_time作为watermark
    val table = params.kafkaTableName

    // flink 1.11  default catalog 不支持create table if not exists 1.12支持
    // flink 1.11 不支持create table 时带上数据库名称，1.12支持
    // 如果存在表删除
    tEnv.executeSql("DROP TABLE IF EXISTS " + table)
    val sourceTableSQL =
      s"""create table  $table(
         |  uuid string,
         |  `date` string,
         |  ad_type int,
         |  ad_type_name string,
         |  `timestamp` bigint,
         |event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`/1000,'yyyy-MM-dd HH:mm:ss')),
         |WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
         |)with(
         |'connector' = 'kafka',
         |'topic' = '${params.sourceTopic}',
         |'properties.bootstrap.servers'='${params.brokerList}',
         |'properties.group.id' = '${params.groupId}',
         |'format' = 'json',
         |'json.fail-on-missing-field' = 'false',
         |'json.ignore-parse-errors' = 'true'
         |)
      """.stripMargin
    // 创建表，会在hive metastore中存储
    tEnv.executeSql(sourceTableSQL)

    // 切换为hive dialect, 创建hive表，设置为三级分区，day,hour,min
    tEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    val hiveTable = params.hiveTableName
    // 注意虽然设置了auto-compaction参数，即自动小文件合并，但是Flink 1.11 没有实现，Flink 1.12中已经实现
    // 注意下面建表的location需要在flink 1.11.2需要s3a,bug https://issues.apache.org/jira/browse/FLINK-19144，在flink 1.11.4修复
    val createHiveTableSQL =
    s"""
       |  CREATE  EXTERNAL TABLE  if not exists $hiveTable(
       |  uuid string,
       | `date` string,
       |  ad_type int,
       |  ad_type_name string,
       | `timestamp` bigint
       |    ) PARTITIONED BY (logday STRING,h STRING,m STRING)
       |    STORED AS parquet
       |    LOCATION '${params.hiveS3Path}'
       |    TBLPROPERTIES (
       |    'partition.time-extractor.timestamp-pattern' = '$$logday $$h:$$m:00',
       |    'sink.partition-commit.trigger'='partition-time',
       |    'sink.partition-commit.delay'='1 min',
       |    'sink.partition-commit.policy.kind'='metastore',
       |    'sink.rolling-policy.rollover-interval'='1 min',
       |    'sink.rolling-policy.file-size'='128MB',
       |    'auto-compaction'='true'
       |    )
       |
      """.stripMargin
    tEnv.executeSql(createHiveTableSQL)

    //insert data to hive(s3)
    val insertDataSQL =
      s"""
         |INSERT INTO TABLE $hiveTable
         |SELECT
         |  uuid ,
         | `date` ,
         |  ad_type ,
         |  ad_type_name ,
         | `timestamp` ,
         |DATE_FORMAT(event_time, 'yyyy-MM-dd') as logday,
         |DATE_FORMAT(event_time, 'HH') as h,
         |DATE_FORMAT(event_time,'mm') as m
         |FROM $table
      """.stripMargin

    tEnv.executeSql(insertDataSQL)
  }
}
