package com.xiaomi.infra.pegasus.spark.bulkloader.examples

import com.xiaomi.infra.pegasus.spark.bulkloader.StartBulkloadInfo
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException
import com.xiaomi.infra.pegasus.spark.common.utils.gateway.{Cluster, Compaction}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import org.apache.log4j.{Level, Logger}

object SstFileLoadPegasusRunner {
  val logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(
        "pegasus spark load sst to table"
      )
      .setIfMissing("spark.master", "local[1]")
    var sc = new SparkContext(conf)

    val task = conf.get("spark.pegasus.task")
    if (!task.equals("import")) {
      throw new PegasusSparkException("SstFileLoadPegasusRunner must set --conf spark.pegasus.task=import")
    }

    val cluster = conf.get("spark.pegasus.import.cluster", "c4tst-function1")
    val tableName = conf.get("spark.pegasus.import.table", "usertable_bulkload")
    val sstFileProvider = conf.get("spark.pegasus.import.sst.hdfs", "hdfs_zjCluster.java:301y")
    val sstFilePath = conf.get("spark.pegasus.import.sst.path", "/tmp")
    val enableDetectDupStr = conf.get("spark.pegasus.import.enableDetectDup", "false")
    val enableDetectDup = enableDetectDupStr.toBoolean

    // todo just for zili, need delete it
    if (cluster.contains("azmb")) {
      Cluster.metaGateWay = "http://pegasus-gateway-cl11020.aws-mb.ingress.mice.cc.d.xiaomi.net"
    }

    val resultMap: ConcurrentHashMap[String, java.lang.Boolean] = Cluster.startBulkLoad(cluster, tableName, sstFileProvider, sstFilePath, new Compaction("03:00", 1, false), enableDetectDup)
    val scalaResultMap = resultMap.asScala
    scalaResultMap.foreach { case (key, value) =>
      logger.info(s"Cluster:Name: $key, BulkloadResult: $value")
    }
  }
}