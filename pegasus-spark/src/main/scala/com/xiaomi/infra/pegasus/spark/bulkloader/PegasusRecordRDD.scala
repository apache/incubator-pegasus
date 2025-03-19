package com.xiaomi.infra.pegasus.spark.bulkloader

import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException
import com.xiaomi.infra.pegasus.spark.common.utils.JNILibraryLoader
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

/**
  * PegasusRecordRDD is a data set that can be persisted into Pegasus via BulkLoad.
  */
class PegasusRecordRDD(data: RDD[(PegasusKey, PegasusValue)]) {
  private val LOG = LogFactory.getLog(classOf[PegasusRecordRDD])

  /**
    * Transform this data set into Pegasus files, which can be directly
    * ingested into Pegasus's storage engine.
    */
  def saveAsPegasusFile(config: BulkLoaderConfig): Unit = {
    checkTablePathExist(config)

    var rdd = data
    if (config.getAdvancedConfig.enableDistinct) {
      rdd = rdd.reduceByKey((value1, value2) => value2)
    }

    if (config.getAdvancedConfig.enableSort) {
      rdd = rdd.repartitionAndSortWithinPartitions(
        new PegasusHashPartitioner(config.getTablePartitionCount)
      )
    } else {
      rdd = rdd.partitionBy(
        new PegasusHashPartitioner(config.getTablePartitionCount)
      )
    }

    rdd.foreachPartition(i => {
      JNILibraryLoader.load()
      new BulkLoader(config, i.asJava, TaskContext.getPartitionId()).start()
    })

    if (config.isEnableValidateAfterGenerate) {
      checkPartitionFolderValid(config)

      rdd.foreachPartition(i => {
        JNILibraryLoader.load()
        new BulkLoader(config, i.asJava, TaskContext.getPartitionId())
          .validateGenerateFiles()
      })
    }

    val tablePath =
      config.getRemoteFileSystemURL + "/" + config.getRemoteFileSystemPath +
        "/" + config.getClusterName + "/" + config.getTableName

    LOG.info(
      config.getClusterName + "." + config.getTableName + " data has generated on " + tablePath
    )
  }

  // not allow generate data in same path which usually has origin data
  private def checkTablePathExist(config: BulkLoaderConfig): Unit = {
    val tablePath =
      config.getRemoteFileSystemURL + "/" + config.getRemoteFileSystemPath +
        "/" + config.getClusterName + "/" + config.getTableName
    val remoteFileSystem = config.getRemoteFileSystem

    if (remoteFileSystem.exist(tablePath)) {
      throw new PegasusSparkException(
        "the data [" + tablePath + "] has been existed, please make sure put different path!"
      )
    }
  }

  private def checkPartitionFolderValid(config: BulkLoaderConfig): Unit = {
    val tablePath =
      config.getRemoteFileSystemURL + "/" + config.getRemoteFileSystemPath +
        "/" + config.getClusterName + "/" + config.getTableName
    val remoteFileSystem = config.getRemoteFileSystem
    if (!remoteFileSystem.exist(tablePath + "/" + BulkLoader.BULK_LOAD_INFO)) {
      LOG.error(
        String.format(
          "can't find %s file, will re-generate it",
          tablePath + "/" + BulkLoader.BULK_LOAD_INFO
        )
      )
      new BulkLoader(config, null, TaskContext.getPartitionId())
        .createBulkLoadInfoFile()
    }

    val fileCount = remoteFileSystem.getFileStatus(tablePath).length
    if (fileCount != config.getTablePartitionCount + 1) {
      throw new PegasusSparkException(
        "the data[" + tablePath + "] is not completed, partition_count expect vs actual = " + config.getTablePartitionCount + ":"
          + (fileCount - 1)
      )
    } else {
      LOG.info(
        "validate the data[" + tablePath + "] folder completed, partition_count expect vs actual = " + config.getTablePartitionCount + ":"
          + (fileCount - 1)
      )
    }
  }
}
