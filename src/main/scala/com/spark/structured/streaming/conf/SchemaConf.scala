package com.spark.structured.streaming.conf

case class SchemaConf(name: String, dataType: String, precision: Int, scale: Int, nullable: Boolean = true)
