package com.spark.structured.streaming.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object Common {

  def initSparkSession(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).enableHiveSupport.getOrCreate()
  }


  def loadConfig(nameSpace: String = "kafka-hive"): Config = {
    ConfigFactory.defaultApplication().getConfig(nameSpace)
  }

}
