package com.spark.structured.streaming

import com.typesafe.config.{Config, ConfigFactory}

package object conf {

  def buildAppConfig(conf: Config): AppConf = {
    val kafkaBootStrapServers = conf.getString("kafka.bootstrap-servers")
    val kafkaTopic = conf.getString("kafka.topic")
    val kafkaBatchInterval = conf.getString("kafka.batch-interval")
    val kafkaClientId = conf.getString("kafka.client-id")
    val kafkaSSLProtocol = conf.getString("kafka.ssl-protocol")
    val kafkaOffset = conf.getString("kafka.offset-strategy")
    val kafkaCheckpointLocation = conf.getString("kafka.checkpoint-path")

    val hiveDbName = conf.getString("hive.db-name")
    val hiveTable = conf.getString("hive.table-name")
    val writeMode = conf.getString("hive.write-mode")

    val hiveConf = HiveConf(hiveDbName, hiveTable, writeMode)
    val kafkaConf = KafkaConf(kafkaBootStrapServers, kafkaTopic, kafkaClientId, kafkaBatchInterval, kafkaSSLProtocol, kafkaOffset, kafkaCheckpointLocation)
    val schemaConf = buildSchemaConfig(conf.getConfig("schema"))
    AppConf(conf.getString("app-name"), kafkaConf, hiveConf, schemaConf)
  }

  private def buildSchemaConfig(config: Config) = {
    import collection.JavaConverters._
    config.getConfigList("fields").asScala.toList.map(conf => {
      val fieldName = conf.getString("name")
      val dataType = conf.getString("data-type")
      val precision = if (conf.hasPath("precision")) conf.getInt("precision") else 0
      val scale = if (conf.hasPath("scale")) conf.getInt("scale") else 0
      SchemaConf(fieldName, dataType, precision, scale)
    })
  }

  def loadConfig(nameSpace: String = "kafka-hive"): Config = {
    ConfigFactory.defaultApplication().getConfig(nameSpace)
  }


}
