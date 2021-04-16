package com.aws.analytics.state

import com.aws.analytics.conf.Config
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{ StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.slf4j.{Logger, LoggerFactory}


/**
 * only test state clean
 */
object TableStateClean {
  val LOG: Logger = LoggerFactory.getLogger(TableStateClean.getClass)

  def main(args: Array[String]): Unit = {
    val params = Config.parseConfig(TableStateClean, args)

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
    val table = params.kafkaTableName

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
    tEnv.executeSql(sourceTableSQL)
    tEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    tEnv.getConfig.setIdleStateRetentionTime(Time.minutes(10),Time.minutes(20))
    val insertDataSQL =
      s"""
         select a.uuid,b.uuid from $table as a
          inner join $table  as b
          on a.uuid = b.uuid
      """.stripMargin
    tEnv.executeSql(insertDataSQL)
  }
}
