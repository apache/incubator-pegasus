package com.xiaomi.infra.pegasus.spark.analyser.examples.parquet

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Schema {
  val struct = StructType(
    Array(
      StructField("hashKey", StringType),
      StructField("sortKey", StringType),
      StructField("value", StringType)
    )
  )

}
