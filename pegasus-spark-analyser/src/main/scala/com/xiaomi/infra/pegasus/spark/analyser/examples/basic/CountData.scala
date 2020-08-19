package com.xiaomi.infra.pegasus.spark.analyser.examples.basic

import com.xiaomi.infra.pegasus.spark.FDSConfig
import com.xiaomi.infra.pegasus.spark.analyser.{
  ColdBackupConfig,
  ColdBackupLoader
}
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._

object CountData {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("count data")
      .setIfMissing("spark.master", "local[1]")
    // if data in HDFS, pass HDFSConfig()
    val coldBackupConfig =
      new ColdBackupConfig(
        new FDSConfig(
          "accessKey",
          "accessSecret",
          "bucketName",
          "endPoint"
        ),
        "clusterName",
        "tableName"
      )

    var count = 0
    val sc = new SparkContext(conf)
      .pegasusSnapshotRDD(coldBackupConfig)
      .map(i => {
        count = count + 1
        if (count % 10000 == 0) {
          println("count=" + count)
        }
      })
      .count()

  }

}
