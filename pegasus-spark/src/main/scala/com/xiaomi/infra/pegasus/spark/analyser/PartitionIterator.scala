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

import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext

/**
  * Iterator of a pegasus partition. It's used to load the entire partition sequentially
  * and sorted by bytes-order.
  */
private[analyser] class PartitionIterator private (
    context: TaskContext,
    val pid: Int
) extends Iterator[PegasusRecord]
    with AutoCloseable {

  private val LOG = LogFactory.getLog(classOf[PartitionIterator])

  private var pegasusScanner: PegasusScanner = _

  private var closed = false
  private var thisRecord: PegasusRecord = _
  private var nextRecord: PegasusRecord = _

  private var name: String = _
  // TODO(wutao1): add metrics for counting the number of iterated records.

  def this(context: TaskContext, snapshotLoader: PegasusLoader, pid: Int) {
    this(context, pid)

    pegasusScanner = snapshotLoader.getScanner(pid)
    pegasusScanner.seekToFirst()
    assert(pegasusScanner.isValid)
    pegasusScanner.next() // skip the first record
    if (pegasusScanner.isValid) {
      nextRecord = pegasusScanner.restore()
    }
    name = "PartitionIterator[pid=%d]".format(pid)
  }

  override def close() {
    if (!closed) {
      // release the C++ pointers
      pegasusScanner.close()
      closed = true
      LOG.info(toString() + " closed")
    }
  }

  override def hasNext: Boolean = {
    nextRecord != null && !closed
  }

  override def next(): PegasusRecord = {
    thisRecord = nextRecord
    pegasusScanner.next()
    if (pegasusScanner.isValid) {
      nextRecord = pegasusScanner.restore()
    } else {
      nextRecord = null
    }
    thisRecord
  }

  override def toString(): String = {
    name
  }
}
