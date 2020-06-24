package com.xiaomi.infra.pegasus.spark.analyser.recipes.parquet

import com.xiaomi.infra.pegasus.spark.analyser.{
  ColdBackupConfig,
  ColdBackupLoader,
  PegasusContext
}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object ConvertParquet {

  private val FS_URL = ""
  private val FS_PORT = "80"
  private val CLUSTER_NAME = ""
  private val TABLE_NAME = ""

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("convertParquet")
      .master("local[1]")
      .getOrCreate()

    val coldBackupConfig =
      new ColdBackupConfig(FS_URL, FS_PORT, CLUSTER_NAME, TABLE_NAME);

    val pc = new PegasusContext(spark.sparkContext)
    val rdd = pc.pegasusSnapshotRDD(new ColdBackupLoader(coldBackupConfig))

    // please make sure your data can be converted valid string value
    val dataFrame = spark.createDataFrame(
      rdd.map(i =>
        Row(new String(i.hashKey), new String(i.sortKey), new String(i.value))
      ),
      Schema.struct
    )

    dataFrame
      .coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      // in online, you need save on hdfs
      .save("/tmp")

    //after convert success(only test)
    val dataFrameResult = spark.read.parquet("/tmp")
    dataFrameResult.show(10)
  }
}
