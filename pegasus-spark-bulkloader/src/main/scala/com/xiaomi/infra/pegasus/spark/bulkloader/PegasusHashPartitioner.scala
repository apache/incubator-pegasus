package com.xiaomi.infra.pegasus.spark.bulkloader

import com.xiaomi.infra.pegasus.spark.Tools
import org.apache.spark.Partitioner

/**
  * The custom hash rule for pegasus
  * @param num hash partition, equal with the pegasus table partition count
  */
class PegasusHashPartitioner(val num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    Tools
      .remainderUnsigned(Tools.hash(key.asInstanceOf[PegasusBytes].data), num)
      .toInt
  }
}
