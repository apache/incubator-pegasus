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

import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._
import com.xiaomi.infra.pegasus.spark.utils.JNILibraryLoader
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

class PegasusRecordRDD(data: RDD[(PegasusKey, PegasusValue)]) {
  private val LOG = LogFactory.getLog(classOf[PegasusRecordRDD])

  def saveAsPegasusFile(config: BulkLoaderConfig): Unit = {
    checkExistAndDelete(config)

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
  }

  // if has older bulkloader data, need delete it
  // TODO(jiashuo) the logic may need be deleted
  private def checkExistAndDelete(config: BulkLoaderConfig): Unit = {
    val tablePath = config.getRemoteFileSystemURL + "/" +
      config.getDataPathRoot + "/" + config.getClusterName + "/" + config.getTableName
    val remoteFileSystem = config.getRemoteFileSystem

    if (remoteFileSystem.exist(tablePath)) {
      LOG.warn(
        "the data " + tablePath + " has been existed, and will be deleted!"
      )
      remoteFileSystem.delete(tablePath, true)
    }
  }

}
