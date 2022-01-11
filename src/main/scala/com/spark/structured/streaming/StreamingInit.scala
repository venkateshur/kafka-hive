package com.spark.structured.streaming


import com.spark.structured.streaming.conf.{buildAppConfig, loadConfig}
import com.spark.structured.streaming.model.process.Processor
import com.spark.structured.streaming.model.read.Reader
import com.spark.structured.streaming.model.write.Writer
import com.spark.structured.streaming.util.Common.{buildSchema, initSparkSession}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}


object StreamingInit extends App {

  private val logger = LoggerFactory.getLogger("kafka to hive data ingestion")
  implicit val spark: SparkSession = initSparkSession("kafka to hive data ingestion")

  Try {
    val config = loadConfig()
    val appConfig = buildAppConfig(config)
    spark.conf.set("spark.app.name", appConfig.appName)
    val readKafkaStream = Reader.kafkaConsumer(appConfig.kafkaConf)
    readKafkaStream.writeStream.foreachBatch{(df: Dataset[Row], _: Long) => {
      val hiveColumns = Reader.hiveReader(appConfig.hiveConf).columns
      Writer.hiveWriter(Processor(df, buildSchema(appConfig.schemaConf), hiveColumns), appConfig.hiveConf)}
    }
      .option("checkpointLocation", s"appConfig.kafkaCheckpointLocation/${appConfig.kafkaConf.topic}/${appConfig.hiveConf.tableName}")
      .trigger(Trigger.ProcessingTime(s"${appConfig.kafkaConf.batchInterval} seconds"))
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


}
