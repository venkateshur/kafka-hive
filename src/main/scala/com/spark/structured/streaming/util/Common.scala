package com.spark.structured.streaming.util

import com.spark.structured.streaming.conf.SchemaConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Common {

  def initSparkSession(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).enableHiveSupport.getOrCreate()
  }


  def buildSchema(schemaConfList: Seq[SchemaConf]): StructType = {
    val schemaFields = schemaConfList.map(schemaConf => {
      val dataType = schemaConf.dataType match {
        case "long" => LongType
        case "double" => DoubleType
        case "float" => FloatType
        case "decimal" => DecimalType.apply(schemaConf.precision, schemaConf.scale)
        case _ => StringType
      }
      StructField(schemaConf.name, dataType, nullable = schemaConf.nullable)
    })
    StructType.apply(schemaFields)
  }
}
