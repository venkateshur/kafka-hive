package com.spark.structured.streaming.model.read

import com.spark.structured.streaming.conf.{HiveConf, KafkaConf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Reader {
  def kafkaConsumer(kafkaConf: KafkaConf)(implicit spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConf.kafkaServers)
      .option("subscribe", kafkaConf.topic)
      .option("kafka.security.protocol", kafkaConf.sslProtocol)
      .option("startingOffsets", kafkaConf.offsetStrategy)
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)].select("value")
  }

  def hiveReader(hiveConf: HiveConf)(implicit spark: SparkSession): DataFrame = {
    spark.read.table(s"${hiveConf.dbName}.${hiveConf.tableName}")
  }
}
