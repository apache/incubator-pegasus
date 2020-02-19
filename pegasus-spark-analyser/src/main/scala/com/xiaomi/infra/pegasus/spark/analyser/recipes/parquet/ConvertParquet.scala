package com.xiaomi.infra.pegasus.spark.analyser.recipes.parquet

import com.xiaomi.infra.pegasus.spark.analyser.{ColdBackupConfig, ColdBackupLoader, PegasusContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object ConvertParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("convertParquet")
      .master("local[1]")
      .getOrCreate()

    val coldBackupConfig = new ColdBackupConfig()


    coldBackupConfig.setRemote(
      "",
      "80")
      .setTableInfo("c3srv-browser", "alchemy_feed_exchange_record")

    val pc = new PegasusContext(spark.sparkContext)
    val rdd = pc.pegasusSnapshotRDD(new ColdBackupLoader(coldBackupConfig))

    // please make sure your data can be converted valid string value
    val dataFrame = spark.createDataFrame(
      rdd.map(i =>
        Row(new String(i.hashKey), new String(i.sortKey), new String(i.value))),
      Schema.struct)

    dataFrame
      .coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      // in online, you need save on hdfs
      .save("c3srv-browser_alchemy_feed_exchange_record.parquet")

    //after convert success(only test)
    val dataFrameResult = spark.read.parquet("c3srv-browser_alchemy_feed_exchange_record.parquet")
    dataFrameResult.show(10)
  }
}
