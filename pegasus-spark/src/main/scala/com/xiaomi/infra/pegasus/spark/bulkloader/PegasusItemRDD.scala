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
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

class PegasusSingleItemRDD(resource: RDD[SetItem]) extends Serializable {
  private val logger = LoggerFactory.getLogger(classOf[PegasusSingleItemRDD])

  // batch set the data into pegasus
  def loadIntoPegasus(config: OnlineLoaderConfig): Unit = {
    val partitionCount = resource.getNumPartitions
    resource.foreachPartition(i => {
      val onlineLoader = new OnlineLoader(config, partitionCount)
      val partitionId = TaskContext.getPartitionId

      var currentCount = 0
      i.sliding(config.getBatchCount, config.getBatchCount)
        .foreach(slice => {
          onlineLoader.loadSingleItem(slice.asJava)
          currentCount += slice.size
          if (currentCount % 100000 == 0) {
            logger
              .info(
                "partition(" + partitionId + ") currentCount = " + currentCount
              )
          }
        })
      onlineLoader.close()
    })
  }
}

// TODO(jiashuo1) the code is repeated with `PegasusSingleItemRDD` which may need be refactored
class PegasusMultiItemRDD(resource: RDD[HashKeyData]) extends Serializable {
  private val logger = LoggerFactory.getLogger(classOf[PegasusMultiItemRDD])

  //batch multiSet the data into pegasus
  def loadIntoPegasus(config: OnlineLoaderConfig, ttl: Int = 0): Unit = {
    val partitionCount = resource.getNumPartitions
    resource.foreachPartition(i => {
      val partitionId = TaskContext.getPartitionId
      val onlineLoader = new OnlineLoader(config, partitionCount)

      var currentCount = 0
      i.sliding(config.getBatchCount, config.getBatchCount)
        .foreach(slice => {
          onlineLoader.loadMultiItem(slice.asJava, ttl)
          currentCount += slice.size
          if (currentCount % 100000 == 0) {
            logger
              .info(
                "partition(" + partitionId + ") currentCount = " + currentCount
              )
          }
        })
      onlineLoader.close()
    })
  }
}
