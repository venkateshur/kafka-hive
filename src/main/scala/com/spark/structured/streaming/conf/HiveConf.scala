package com.spark.structured.streaming.conf

case class HiveConf(dbName: String, tableName: String, writeMode: String)
