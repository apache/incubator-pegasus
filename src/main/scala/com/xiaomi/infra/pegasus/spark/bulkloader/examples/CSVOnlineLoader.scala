package com.xiaomi.infra.pegasus.spark.bulkloader.examples

import java.time.Duration

import com.xiaomi.infra.pegasus.client.{ClientOptions, HashKeyData, SetItem}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._
import com.xiaomi.infra.pegasus.spark.bulkloader.OnlineLoaderConfig
import com.xiaomi.infra.pegasus.spark.common.utils.FlowController.RateLimiterConfig
import org.apache.spark.{SparkConf, SparkContext}

object CSVOnlineLoader {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("Convert to Online data into cluster")
      .setIfMissing("spark.master", "local[1]")
    val sc = new SparkContext(conf)

    // This example only shows how to convert CSV file into Pegasus, actually any data source that
    // can be converted RDD can be load into pegasus
    sc.textFile("data.csv")
      /** if resource data format is : one hashkey=>multi value, such as Hbase format, you need create multiSetItems within one hashKey* */
      /** .map(i => {
        *        val lines = i.split(",")
        *        val multiSetItems = new HashKeyData(lines(0).getBytes())
        *        val values:Array[String] = lines(1).split("|")
        *        for(pair <- values) {
        *          val sortKey = pair.split("@")(0).getBytes()
        *          val value = pair.split("@")(1).getBytes()
        *          val ttl = 0
        *          multiSetItems.addData(sortKey, value)
        *        }
        *        multiSetItems
        *      })*
        */
      .map(i => {
        val lines = i.split(",")
        new SetItem(
          lines(0).getBytes(),
          lines(1).getBytes(),
          lines(2).getBytes()
        )
      })
      .loadIntoPegasus(
        new OnlineLoaderConfig(
          ClientOptions
            .builder()
            .metaServers(
              "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603"
            )
            .operationTimeout(Duration.ofMillis(10000))
            .build(),
          "onebox",
          "usertable"
          // Note: The finalQPS = QPS / partitionCount * parallelism. And `partitionCount` is usually
          // equal with `parallelism` generally in spark
        ).setRateLimiterConfig(new RateLimiterConfig().setQps(10000))
      )
  }
}
