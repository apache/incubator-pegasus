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

package com.xiaomi.infra.pegasus.spark.analyser

import com.xiaomi.infra.pegasus.spark.utils.JNILibraryLoader
import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.rocksdb.RocksDB

/**
  * PegasusContext is a serializable container for analysing Pegasus's checkpoint on HDFS.
  *
  * [[PegasusContext]] should be created in the driver, and shared with executors
  * as a serializable field.
  */
// TODO(jiashuo1) refactor rdd/iterator for adding pegasus online data
class PegasusContext(private val sc: SparkContext) extends Serializable {

  def pegasusSnapshotRDD(config: Config): PegasusSnapshotRDD = {

    new PegasusSnapshotRDD(
      this,
      PegasusLoaderFactory.createDataLoader(config),
      sc
    )
  }
}

/**
  * A RDD backed by a FDS snapshot of Pegasus.
  *
  * To construct a PegasusSnapshotRDD, use [[PegasusContext#pegasusSnapshotRDD]].
  */
class PegasusSnapshotRDD private[analyser] (
    pegasusContext: PegasusContext,
    snapshotLoader: PegasusLoader,
    @transient sc: SparkContext
) extends RDD[PegasusRecord](sc, Nil) {

  private val LOG = LogFactory.getLog(classOf[PegasusSnapshotRDD])

  override def compute(
      split: Partition,
      context: TaskContext
  ): Iterator[PegasusRecord] = {
    // Loads the librocksdb library into jvm.
    JNILibraryLoader.load()

    LOG.info(
      "Create iterator for \"%s\" \"%s\" [pid: %d]"
        .format(
          snapshotLoader.getConfig.getClusterName,
          snapshotLoader.getConfig.getTableName,
          split.index
        )
    )
    new PartitionIterator(context, snapshotLoader, split.index)
  }

  override protected def getPartitions: Array[Partition] = {
    val indexes = Array.range(0, snapshotLoader.getPartitionCount)
    indexes.map(i => {
      new PegasusPartition(i)
    })
  }

  def getPartitionCount: Int = {
    snapshotLoader.getPartitionCount
  }

  /**
    * @param other the other PegasusSnapshotRDD with which to diff against
    * @return a RDD representing a different set of records in which none of each
    *         exists in both `this` and `other`.
    */
  def diff(other: PegasusSnapshotRDD): RDD[PegasusRecord] = {
    subtract(other).union(other.subtract(this))
  }
}

/**
  * @param partitionIndex Each spark partition maps to one Pegasus partition.
  */
private class PegasusPartition(partitionIndex: Int) extends Partition {
  override def index: Int = partitionIndex
}
