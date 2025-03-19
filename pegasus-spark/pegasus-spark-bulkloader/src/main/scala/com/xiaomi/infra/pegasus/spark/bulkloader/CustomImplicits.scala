/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
