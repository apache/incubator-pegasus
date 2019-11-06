package com.xiaomi.infra.pegasus.analyser.sample.spark

import com.xiaomi.infra.pegasus.analyser.{Config, FDSService, PegasusClient}
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.rocksdb.RocksDB

object ScanAllData {

  class scanAllData

  private val LOG = LogFactory.getLog(classOf[scanAllData])

  def main(args: Array[String]): Unit = {
    val config = new Config("core-site.xml")
    val fdsService = new FDSService(config, "c3srv-browser", "alchemy_feed_exchange_record")
    val partitionCount = fdsService.getPartitionCount

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
      val pegasusClient = new PegasusClient(config, fdsService)
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
      LOG.info("partitionId: " + TaskContext.getPartitionId() + " has completed")
    })
  }

}
