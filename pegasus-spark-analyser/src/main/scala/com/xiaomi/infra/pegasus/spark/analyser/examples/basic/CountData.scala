package com.xiaomi.infra.pegasus.spark.analyser.examples.basic

import com.xiaomi.infra.pegasus.spark.FDSConfig
import com.xiaomi.infra.pegasus.spark.analyser.{
  ColdBackupConfig,
  ColdBackupLoader,
  PegasusContext
}
import org.apache.spark.{SparkConf, SparkContext};

object CountData {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("count data")
      .setIfMissing("spark.master", "local[1]")
    val sc = new SparkContext(conf)
    // if data in HDFS, pass HDFSConfig()
    val coldBackupConfig =
      new ColdBackupConfig(
        new FDSConfig(
          "accessKey",
          "accessSecret",
          "bucketName",
          "endPoint",
          "port"
        ),
        "clusterName",
        "tableName"
      )

    var count = 0
    new PegasusContext(sc)
      .pegasusSnapshotRDD(new ColdBackupLoader(coldBackupConfig))
      .map(i => {
        count = count + 1
        if (count % 10000 == 0) {
          println("count=" + count)
        }
      })
      .count()

  }

}
