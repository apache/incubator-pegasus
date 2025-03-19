package com.xiaomi.infra.pegasus.spark.analyser.examples.basic

import com.xiaomi.infra.pegasus.spark.FDSConfig
import com.xiaomi.infra.pegasus.spark.analyser.ColdBackupConfig
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._

object CountData {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("count pegasus data stored in XiaoMi's FDS")
      .setIfMissing("spark.master", "local[1]")

    val coldBackupConfig =
      new ColdBackupConfig(
        new FDSConfig( // if data is in HDFS, pass HDFSConfig()
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
      .map(_ => {
        count = count + 1
        if (count % 10000 == 0) {
          println("count=" + count)
        }
      })
      .count()
  }
}
