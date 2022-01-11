package com.spark.structured.streaming.conf

case class AppConf(appName: String, kafkaConf: KafkaConf, hiveConf: HiveConf, schemaConf: Seq[SchemaConf])
