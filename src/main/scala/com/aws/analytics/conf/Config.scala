package com.aws.analytics.conf

case class Config(
                   brokerList: String = "",
                   sourceTopic: String = "",
                   checkpointDir: String ="",
                   output:String = "",
                   groupId:String = "",
                   checkpointInterval:String ="60",
                   rolloverInterval:String="10",
                   inactivityInterval:String="2",
                   maxPartSize:String="1073741824",
                   database:String="default",
                   kafkaTableName:String="log_kafka_flink",
                   hiveTableName:String="source_log",
                   metastore:String="",
                   hiveS3Path:String="",
                   hiveConfDir:String="",

                 )


object Config {

  def parseConfig(obj: Object,args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[Config](programName) {
      head(programName, "1.0")
      opt[String]('b', "brokerList").required().action((x, config) => config.copy(brokerList = x)).text("kafka broker list,sep comma")
      opt[String]('t', "sourceTopic").required().action((x, config) => config.copy(sourceTopic = x)).text("kafka topic")
      opt[String]('g', "groupId").required().action((x, config) => config.copy(groupId = x)).text("comsumer group id")
      opt[String]('c', "checkpointDir").required().action((x, config) => config.copy(checkpointDir = x)).text("checkpoint dir")
      opt[String]('l', "checkpointInterval").optional().action((x, config) => config.copy(checkpointInterval = x)).text("checkpoint interval: default 60 seconds")

      programName match {
        case "Kafka2S3Hive" =>
          opt[String]('m', "metastore").required().action((x, config) => config.copy(metastore = x)).text("hive metastore uri, eg. thrift://*****:9083")
          opt[String]('d', "database").optional().action((x, config) => config.copy(database = x)).text("default: ods_event")
          opt[String]('k', "kafkaTableName").optional().action((x, config) => config.copy(kafkaTableName = x)).text("default: log_kafka_flink")
          opt[String]('n', "hiveTableName").optional().action((x, config) => config.copy(hiveTableName = x)).text("default: event_flink")
          opt[String]('p', "hiveS3Path").required().action((x, config) => config.copy(hiveS3Path = x)).text("hive table s3 path")
          opt[String]('v', "hiveConfDir").required().action((x, config) => config.copy(hiveConfDir = x)).text("hive conf dir,eg: /etc/hive/conf")


        case "Kafka2S3Text" =>
          opt[String]('r', "rolloverInterval").optional().action((x, config) => config.copy(rolloverInterval = x)).text("rolloverInterval: default 10 minutes")
          opt[String]('i', "inactivityInterval").optional().action((x, config) => config.copy(inactivityInterval = x)).text("inactivityInterval: default 2 minutes")
          opt[String]('m', "maxPartSize").optional().action((x, config) => config.copy(maxPartSize = x)).text("maxPartSize: default 1G")
          opt[String]('o', "output").required().action((x, config) => config.copy(output = x)).text("output path")

        case "Kafka2S3Parquet" =>
          opt[String]('o', "output").required().action((x, config) => config.copy(output = x)).text("output path")

        case _ =>

      }


    }
    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        //        println("cannot parse args")
        System.exit(-1)
        null
      }
    }

  }

}
