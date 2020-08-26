package com.xiaomi.infra.pegasus.spark.bulkloader

import CustomImplicits._
import com.xiaomi.infra.pegasus.spark.JNILibraryLoader
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._

class PegasusRecordRDD(data: RDD[(PegasusKey, PegasusValue)]) {

  def saveAsPegasusFile(config: BulkLoaderConfig): Unit = {
    var rdd = data
    if (config.getAdvancedConfig.enableDistinct) {
      rdd = rdd.distinct()
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
  }

}
