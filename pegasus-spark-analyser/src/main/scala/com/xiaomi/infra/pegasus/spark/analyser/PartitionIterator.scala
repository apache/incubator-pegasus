package com.xiaomi.infra.pegasus.spark.analyser

import com.xiaomi.infra.pegasus.spark
import com.xiaomi.infra.pegasus.spark.{Config, RocksDBOptions}
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.rocksdb.{RocksDB, RocksIterator}

/**
  * Iterator of a pegasus partition. It's used to load the entire partition sequentially
  * and sorted by bytes-order.
  */
private[analyser] class PartitionIterator private (
    context: TaskContext,
    val pid: Int
) extends Iterator[PegasusRecord] {

  private val LOG = LogFactory.getLog(classOf[PartitionIterator])

  private var rocksIterator: RocksIterator = _
  private var rocksDB: RocksDB = _
  private var rocksDBOptions: RocksDBOptions = _

  private var closed = false
  private var thisRecord: PegasusRecord = _
  private var nextRecord: PegasusRecord = _

  private var pegasusLoader: PegasusLoader = _

  private var name: String = _
  // TODO(wutao1): add metrics for counting the number of iterated records.

  def this(context: TaskContext, snapshotLoader: PegasusLoader, pid: Int) {
    this(context, pid)

    pegasusLoader = snapshotLoader
    rocksDBOptions = new RocksDBOptions(snapshotLoader.getConfig)
    val checkPointUrls = snapshotLoader.getCheckpointUrls
    val dbPath = checkPointUrls.get(pid)
    rocksDB = RocksDB.openReadOnly(rocksDBOptions.options, dbPath)
    rocksIterator = rocksDB.newIterator(rocksDBOptions.readOptions)
    rocksIterator.seekToFirst()
    assert(rocksIterator.isValid)
    rocksIterator.next() // skip the first record
    if (rocksIterator.isValid) {
      nextRecord = pegasusLoader.restoreRecord(rocksIterator)
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
      rocksIterator.close()
      rocksDB.close()
      rocksDBOptions.close()
      closed = true
      LOG.info(toString() + " closed")
    }
  }

  override def hasNext: Boolean = {
    nextRecord != null && !closed
  }

  override def next(): PegasusRecord = {
    thisRecord = nextRecord
    rocksIterator.next()
    if (rocksIterator.isValid) {
      nextRecord = pegasusLoader.restoreRecord(rocksIterator)
    } else {
      nextRecord = null
    }
    thisRecord
  }

  override def toString(): String = {
    name
  }
}
