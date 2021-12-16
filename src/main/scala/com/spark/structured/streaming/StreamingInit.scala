package com.spark.structured.streaming


import com.spark.structured.streaming.util.Common.{initSparkSession, loadConfig}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class AppConf(kafkaServers: String, topic: String, batchInterval: String, kafkaCheckpointLocation: String, tableName: String, writeMode: String)

object StreamingInit extends App  {

  private val logger = LoggerFactory.getLogger("kafka to hive data ingestion")
  implicit val spark: SparkSession = initSparkSession("kafka to hive data ingestion")

  Try {
    val config = loadConfig("kafka-hive")
    val appConfig = buildAppConfig(config)
    val readKafkaStream = Consumer.kafkaConsumer(spark, appConfig.kafkaServers, appConfig.topic)
    //TODO add logic parse kafka messages using json schema
    readKafkaStream.writeStream.foreachBatch {
        (df, _) =>
          df.write.mode(appConfig.writeMode).saveAsTable(appConfig.tableName)
      }
      .option("checkpointLocation", appConfig.kafkaCheckpointLocation)
      .trigger(Trigger.ProcessingTime(s"${appConfig.batchInterval} seconds"))
      .start()
      .awaitTermination()

  } match {
    case Success(_) =>
      logger.info("Data Transfer Execution Successful")
      spark.stop()
    case Failure(exception) =>
      spark.stop()
      logger.error("Data Transfer Execution with error: " + exception.getLocalizedMessage)
      throw exception
  }

  private def buildAppConfig(conf:Config) = {
    val kafkaBootStrapServers = conf.getString("kafka.bootstrap-servers")
    val kafkaTopic = conf.getString("kafka.topic")
    val kafkaBatchInterval = conf.getString("kafka.batch-interval")
    val kafkaCheckpointLocation = conf.getString("kafka.checkpoint-path")
    val hiveTable = conf.getString("hive.table")
    val writeMode = conf.getString("hive.write-mode")
    AppConf(kafkaBootStrapServers, kafkaTopic, kafkaBatchInterval, kafkaCheckpointLocation, hiveTable, writeMode)
  }
}
