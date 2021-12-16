package com.spark.structured.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}


object Consumer {

  def kafkaConsumer(spark: SparkSession, kafkaServers: String, topic: String): DataFrame = {
    import spark.implicits._
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaServers)
        .option("subscribe",topic)
        .option("kafka.security.protocol", "SSL")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)].select("value")
    }
}
