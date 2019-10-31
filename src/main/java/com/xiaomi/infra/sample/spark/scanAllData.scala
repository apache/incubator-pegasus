package com.xiaomi.infra.sample.spark

import com.xiaomi.infra.PegasusClient
import com.xiaomi.infra.config.Config
import com.xiaomi.infra.service.FdsService
import com.xiaomi.infra.service.db.PegasusOptions
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.rocksdb.RocksDB

object scanAllData {

  class scanAllData

  private val LOG = LogFactory.getLog(classOf[scanAllData])

  def main(args: Array[String]): Unit = {
    val config = new Config("core-site.xml")
    val fdsService = new FdsService(config, "c3srv-browser", "alchemy_feed_exchange_record")
    val partitionCount = fdsService.getPartitionCounter

    val conf = new SparkConf()
      .setAppName("pegasus data analyse")
      .setIfMissing("spark.master", "local[10]")
      .set("spark.executor.instances", partitionCount.toString)

    val sc = new SparkContext(conf)
    val list = 0 until partitionCount
    val data = sc.parallelize(list, partitionCount)

    data.foreachPartition(i => {
      //RocksDB.loadLibrary() must at here
      RocksDB.loadLibrary()
      var result = List[Int]()
      val pegasusOptions = new PegasusOptions(config)
      val pegasusClient = new PegasusClient(pegasusOptions, fdsService)
      val pid = TaskContext.getPartitionId()
      val rocksdbScanner = pegasusClient.getScanner(pid)
      rocksdbScanner.seekToFirst()
      while (rocksdbScanner.isValid) {
        val pegasusKey = rocksdbScanner.key
        val hashKey = new String(pegasusKey.hashKey)
        val sortKey = new String(pegasusKey.sortKey)
        val value = new String(rocksdbScanner.value)
        LOG.info("*********************************[" + pid + "]" + hashKey + ":" + sortKey + "=>" + value)
        rocksdbScanner.next()
      }
      rocksdbScanner.close()
      LOG.info("partitionID: " + TaskContext.getPartitionId() + "has completed")
    })
  }

}
