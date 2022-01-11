package com.spark.structured.streaming.model.write

import com.spark.structured.streaming.conf.HiveConf
import org.apache.spark.sql.DataFrame

object Writer {
  def hiveWriter(df: DataFrame, hiveConf: HiveConf): Unit = {
    df.write.mode(hiveConf.writeMode).saveAsTable(s"${hiveConf.dbName}.${hiveConf.tableName}")

  }

}
