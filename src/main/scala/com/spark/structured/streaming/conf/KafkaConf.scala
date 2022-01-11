package com.spark.structured.streaming.conf

case class KafkaConf(kafkaServers: String, topic: String,
                     clientId: String, batchInterval: String,
                     sslProtocol: String, offsetStrategy: String, kafkaCheckpointLocation: String)
