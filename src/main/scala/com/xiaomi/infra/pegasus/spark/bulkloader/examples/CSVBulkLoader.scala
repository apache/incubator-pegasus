package com.xiaomi.infra.pegasus.spark.bulkloader.examples

import com.xiaomi.infra.pegasus.spark.bulkloader.DataV0
import com.xiaomi.infra.pegasus.spark.bulkloader.BulkLoaderConfig
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._
import com.xiaomi.infra.pegasus.spark.common.HDFSConfig

object CSVBulkLoader {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("pegasus data bulkloader")
      .setIfMissing("spark.master", "local[1]")

    val sc = new SparkContext(conf)

    val config = new BulkLoaderConfig(
      new HDFSConfig("hdfs://"),
      "clusterName",
      "tableName"
    )

    // This example only shows how to convert CSV file to PegasusFile, actually any data source that
    // can be converted RDD can be also converted to PegasusFile.
    // Note: if the partition size > 2G before "saveAsPegasusFile", you need
    // sc.textFile("data.csv").repartition(n), and let the partition size < 2G
    sc.textFile("data.csv")
      .map(i => {
        val lines = i.split(",")
        config.getDataVersion.create(
          lines(0).getBytes(),
          lines(1).getBytes(),
          lines(2).getBytes()
        )
      })
      .saveAsPegasusFile(config)
  }

}
