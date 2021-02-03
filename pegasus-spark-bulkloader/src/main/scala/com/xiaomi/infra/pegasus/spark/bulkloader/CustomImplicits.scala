package com.xiaomi.infra.pegasus.spark.bulkloader

import com.xiaomi.infra.pegasus.client.{HashKeyData, SetItem}
import com.xiaomi.infra.pegasus.spark.utils.Comparator
import org.apache.spark.rdd.RDD

/**
  * custom implicits object, you can:
  *
  * import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._
  *
  * to use it.
  */
object CustomImplicits {

  /**
    * The implicit implement of ordering by PegasusKey
    */
  implicit val basePegasusKey: Ordering[PegasusKey] =
    new Ordering[PegasusKey] {
      override def compare(x: PegasusKey, y: PegasusKey): Int = {
        Comparator.bytesCompare(x.data, y.data)
      }
    }

  /**
    * The implicit method of converting RDD[(PegasusKey,PegasusValue) to PegasusRecordRDD
    * @param rdd
    * @return
    */
  implicit def convert2PegasusRecordRDD(
      rdd: RDD[(PegasusKey, PegasusValue)]
  ): PegasusRecordRDD =
    new PegasusRecordRDD(rdd)

  /**
    * The implicit method of converting RDD[SetItem) to PegasusSetItemRDD
    * @param rdd
    * @return PegasusSetItemRDD
    */
  implicit def convertFromSingleItem(rdd: RDD[SetItem]): PegasusSingleItemRDD =
    new PegasusSingleItemRDD(rdd)

  /**
    * The implicit method of converting RDD[Array[SetItem]] to PegasusSetItemRDD
    * @param rdd
    * @return PegasusSetItemRDD
    */
  implicit def convertFromMultiItem(
      rdd: RDD[HashKeyData]
  ): PegasusMultiItemRDD =
    new PegasusMultiItemRDD(rdd)
}
