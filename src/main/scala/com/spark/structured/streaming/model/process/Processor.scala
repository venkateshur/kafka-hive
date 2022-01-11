package com.spark.structured.streaming.model.process

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType

object Processor {
  def apply(kafkaDf: Dataset[Row], schema: StructType, tableColumns: Seq[String]): DataFrame = {
    kafkaDf.withColumn("parsedMessage", from_json(col("value"), schema))
      .select(col("parsedMessage.*")).select(tableColumns.map(col): _*)
  }

}
