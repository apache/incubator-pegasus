package com.xiaomi.infra.pegasus.spark.analyser.examples.parquet

import com.xiaomi.infra.pegasus.spark.analyser.{ColdBackupConfig, DataV0}
import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._
import com.xiaomi.infra.pegasus.spark.common.FDSConfig
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object ConvertParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("convertParquet")
      .master(
        "local[1]"
      ) // this config only for test at local, remove it before deploy in cluster
      .getOrCreate()

    // if data in HDFS, pass HDFSConfig()
    val coldBackupConfig =
      new ColdBackupConfig(new FDSConfig("", "", "", "", ""), "onebox", "temp")
      // `initDataVersion` means auto set data version
      // from pegasus-gateway(https://git.n.xiaomi.com/pegasus/pegasus-gateway),
      // if your cluster not support gateway, you need use setDataVersion(), default is dataV1
        .initDataVersion()

    val rdd = spark.sparkContext.pegasusSnapshotRDD(coldBackupConfig)

    // please make sure data can be converted valid string value
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
      // in online, need save on hdfs
      .save("/tmp")

    //after convert success(only test)
    val dataFrameResult = spark.read.parquet("/tmp")
    dataFrameResult.show(10)
  }
}
