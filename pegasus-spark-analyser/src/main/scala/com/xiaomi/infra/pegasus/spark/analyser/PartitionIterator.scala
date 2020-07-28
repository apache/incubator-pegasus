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
) extends Iterator[PegasusRecord] {

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

    // Register an on-task-completion callback to release the resources.
    context.addTaskCompletionListener { context =>
      close()
    }
  }

  private def close() {
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
