package com.xiaomi.infra.pegasus.spark.analyser

import java.nio.ByteBuffer
import java.util

import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.commons.lang3.Validate
import org.apache.commons.lang3.tuple.ImmutablePair
import org.rocksdb.RocksIterator

object PegasusRecord {
  private def restoreKey(
      key: Array[Byte]
  ): ImmutablePair[Array[Byte], Array[Byte]] = {
    Validate.isTrue(key != null && key.length >= 2)
    val buf = ByteBuffer.wrap(key)
    val hashKeyLen = 0xFFFF & buf.getShort
    Validate.isTrue(hashKeyLen != 0xFFFF && (2 + hashKeyLen <= key.length))
    new ImmutablePair[Array[Byte], Array[Byte]](
      util.Arrays.copyOfRange(key, 2, 2 + hashKeyLen),
      util.Arrays.copyOfRange(key, 2 + hashKeyLen, key.length)
    )
  }

  private def restoreValue(value: Array[Byte]): Array[Byte] =
    util.Arrays.copyOfRange(value, 4, value.length)

  def create(rocksIterator: RocksIterator): PegasusRecord = {
    val keyPair = PegasusRecord.restoreKey(rocksIterator.key)
    new PegasusRecord(
      keyPair.getLeft,
      keyPair.getRight,
      PegasusRecord.restoreValue(rocksIterator.value)
    )
  }
}

case class PegasusRecord private (
    hashKey: Array[Byte],
    sortKey: Array[Byte],
    value: Array[Byte]
) {
  override def toString: String =
    String.format(
      "[HashKey=%s, SortKey=%s, Value=%s]",
      util.Arrays.toString(hashKey),
      util.Arrays.toString(sortKey),
      util.Arrays.toString(value)
    )

  override def equals(other: Any): Boolean = {
    other match {
      case that: PegasusRecord =>
        (that canEqual this) &&
          hashKey.sameElements(that.hashKey) &&
          sortKey.sameElements(that.sortKey) &&
          value.sameElements(that.value)
      case _ => false
    }
  }

  override def hashCode: Int = {
    new HashCodeBuilder()
      .append(hashKey)
      .append(sortKey)
      .append(value)
      .hashCode()
  }
}
