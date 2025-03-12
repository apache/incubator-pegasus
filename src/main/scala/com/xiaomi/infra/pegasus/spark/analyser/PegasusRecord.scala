package com.xiaomi.infra.pegasus.spark.analyser

import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException
import com.xiaomi.infra.pegasus.tools.Tools

import java.nio.ByteBuffer
import java.util
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.commons.lang3.Validate
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.commons.logging.{Log, LogFactory}
import org.rocksdb.RocksIterator

abstract class DataVersion extends Serializable {

  def restoreKey(key: Array[Byte]): ImmutablePair[Array[Byte], Array[Byte]] = {
    Validate.isTrue(key != null && key.length >= 2)
    val buf = ByteBuffer.wrap(key)
    val hashKeyLen = 0xffff & buf.getShort
    Validate.isTrue(hashKeyLen != 0xffff && (2 + hashKeyLen <= key.length))
    new ImmutablePair[Array[Byte], Array[Byte]](
      util.Arrays.copyOfRange(key, 2, 2 + hashKeyLen),
      util.Arrays.copyOfRange(key, 2 + hashKeyLen, key.length)
    )
  }

  def restoreExpireTs(value: Array[Byte]): Int = {
    val bytes = util.Arrays.copyOfRange(value, 0, 4)
    ByteBuffer.wrap(bytes).getInt()
  }

  def restoreTimestamp(value: Array[Byte]): Int

  def restoreValue(value: Array[Byte]): Array[Byte]

  def restore(rocksIterator: RocksIterator): PegasusRecord = {
    val keyPair = restoreKey(rocksIterator.key)
    PegasusRecord(
      keyPair.getLeft,
      keyPair.getRight,
      restoreValue(rocksIterator.value),
      restoreExpireTs(rocksIterator.value),
      restoreTimestamp(rocksIterator.value)
    )
  }
}

class DataV0 extends DataVersion {

  def restoreTimestamp(value: Array[Byte]): Int = {
    0
  }

  def restoreValue(value: Array[Byte]): Array[Byte] =
    util.Arrays.copyOfRange(value, 4, value.length)

  override def toString: String = "0"

}

class DataV1 extends DataVersion {

  def restoreTimestamp(value: Array[Byte]): Int = {
    val bytes = util.Arrays.copyOfRange(value, 4, 12)
    ByteBuffer.wrap(bytes).getInt()
  }

  def restoreValue(value: Array[Byte]): Array[Byte] =
    util.Arrays.copyOfRange(value, 12, value.length)

  override def toString: String = "1"

}

class AutoDetectDataVersion extends DataVersion {
  private val LOG = LogFactory.getLog(classOf[DataVersion])

  var validDataVersions = List(new DataV0(), new DataV1())

  var curDataVersionIndex = 0
  var isFirstRecord = true
  var curDataVersion: DataVersion = validDataVersions.head

  def restoreTimestamp(value: Array[Byte]): Int = {
    curDataVersion.restoreTimestamp(value)
  }

  def restoreValue(value: Array[Byte]): Array[Byte] = {
    try {
      val v = curDataVersion.restoreValue(value)
      // every value need be checked to avoid some corner case that some value just can be restore well via selected
      // version, but the other can't
      if (checkIfMessyCode(v)) {
        if (!isFirstRecord) {
          // the data version is selected when first record can be restored well, if the following record is restored
          // failed, it means the auto detect via first record is failed for some corner case and need throw exception
          throw new NumberFormatException(
            "current data version can't restore the value, but not be found at before, version=" + curDataVersion.toString
          )
        }
        if (curDataVersionIndex == validDataVersions.length) {
          throw new ArrayIndexOutOfBoundsException(
            "all the data version can't restore the value, please make sure whether the value is compressed"
          )
        }
        switchDataVersion()
        restoreValue(value)
      } else {
        isFirstRecord = false
        v
      }
    } catch {
      case e: NumberFormatException => {
        throw e
      }
      case e: ArrayIndexOutOfBoundsException => {
        throw e
      }
      case _: Exception => {
        switchDataVersion()
        restoreValue(value)
      }
    }
  }

  def switchDataVersion(): Unit = {
    LOG.warn(
      "current data version can't restore the value, switch next version. current = " + curDataVersion.toString
    )
    curDataVersionIndex += 1
    curDataVersion = validDataVersions(
      curDataVersionIndex % validDataVersions.length
    )
  }

  // if bytes is not messy code, the string result usually can be reversible
  def checkIfMessyCode(value: Array[Byte]): Boolean = {
    !new String(value).getBytes().sameElements(value)
  }

  override def toString: String = "compatible:" + curDataVersion.toString

}

case class PegasusRecord(
    hashKey: Array[Byte],
    sortKey: Array[Byte],
    value: Array[Byte],
    expireTs: Int,
    timestamp: Int
) {
  override def toString: String =
    String.format(
      "[HashKey=%s, SortKey=%s, Value=%s, ExpireTs=%s, Timestamp=%s]",
      util.Arrays.toString(hashKey),
      util.Arrays.toString(sortKey),
      util.Arrays.toString(value),
      String.valueOf(expireTs),
      String.valueOf(timestamp)
    )

  override def equals(other: Any): Boolean = {
    other match {
      case that: PegasusRecord =>
        (that canEqual this) &&
          hashKey.sameElements(that.hashKey) &&
          sortKey.sameElements(that.sortKey) &&
          value.sameElements(that.value) &&
          expireTs == that.expireTs &&
          timestamp == that.timestamp
      case _ => false
    }
  }

  override def hashCode: Int = {
    new HashCodeBuilder()
      .append(hashKey)
      .append(sortKey)
      .append(value)
      .append(expireTs)
      .append(timestamp)
      .hashCode()
  }

  def isExpired: Boolean = {
    this.expireTs != 0 && this.expireTs - Tools.epoch_now().intValue() <= 0
  }
}
