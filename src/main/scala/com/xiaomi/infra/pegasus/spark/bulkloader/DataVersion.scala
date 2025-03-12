package com.xiaomi.infra.pegasus.spark.bulkloader

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import com.google.common.primitives.Bytes
import org.apache.commons.lang3.Validate
import org.apache.commons.lang3.builder.HashCodeBuilder

abstract class DataVersion extends Serializable {

  private val EPOCH_BEGIN = 1451606400 // seconds since 2016.01.01-00:00:00 GMT

  private def generateKey(
      hashKey: Array[Byte],
      sortKey: Array[Byte]
  ): Array[Byte] = {
    val hashKeyLen =
      if (hashKey == null) 0
      else hashKey.length
    Validate.isTrue(
      hashKeyLen < 65535,
      "length of hash key must be less than UINT16_MAX",
      new Array[AnyRef](0)
    )
    val sortKeyLen =
      if (sortKey == null) 0
      else sortKey.length
    val buf = ByteBuffer.allocate(2 + hashKeyLen + sortKeyLen)
    buf.putShort(hashKeyLen.toShort)
    if (hashKeyLen > 0) buf.put(hashKey)

    if (sortKeyLen > 0) buf.put(sortKey)

    buf.array
  }

  def epochNow: Long = {
    val d = new Date
    d.getTime / 1000 - EPOCH_BEGIN
  }

  def Int2Bytes(i: Int): Array[Byte] = {
    val b = ByteBuffer.allocate(4)
    b.order(ByteOrder.BIG_ENDIAN)
    b.putInt(i)
    b.array
  }

  def Long2Bytes(i: Long): Array[Byte] = {
    val b = ByteBuffer.allocate(8)
    b.order(ByteOrder.BIG_ENDIAN)
    b.putLong(i)
    b.array
  }

  def generateValue(
      value: Array[Byte],
      ttl: Int = 0,
      ts: Long = 0,
      clusterId: Short = 0,
      deleteTag: Boolean = false
  ): Array[Byte]

  def create(
      hashKey: Array[Byte],
      sortKey: Array[Byte],
      value: Array[Byte],
      ttl: Int = 0,
      ts: Long = 0,
      clusterId: Short = 0,
      deleteTag: Boolean = false
  ): (PegasusKey, PegasusValue) = {
    (
      PegasusKey(generateKey(hashKey, sortKey)),
      PegasusValue(generateValue(value, ttl, ts, clusterId, deleteTag))
    )
  }

  def create(
      hashKey: Array[Byte],
      sortKey: Array[Byte],
      value: Array[Byte]
  ): (PegasusKey, PegasusValue) = {
    (
      PegasusKey(generateKey(hashKey, sortKey)),
      PegasusValue(generateValue(value))
    )
  }
}

class DataV0 extends DataVersion {

  def generateValue(
      value: Array[Byte],
      ttl: Int = 0,
      ts: Long = 0,
      clusterId: Short = 0,
      deleteTag: Boolean = false
  ): Array[Byte] = {
    if (ttl != 0)
      Bytes.concat(Int2Bytes(ttl + epochNow.toInt), value)
    else Bytes.concat(Int2Bytes(ttl), value)
  }

  override def toString: String = "0"

}

class DataV1 extends DataVersion {

  def generateValue(
      value: Array[Byte],
      ttl: Int = 0,
      ts: Long = 0,
      clusterId: Short = 0,
      deleteTag: Boolean = false
  ): Array[Byte] = {

    val externalTag = Long2Bytes(
      ts << 8 | clusterId << 1 | (if (deleteTag) 1 else 0)
    )
    if (ttl != 0)
      Bytes.concat(Int2Bytes(ttl + epochNow.toInt), externalTag, value)
    else Bytes.concat(Int2Bytes(ttl), externalTag, value)
  }

  override def toString: String = "1"
}

class PegasusBytes(record: Array[Byte]) extends Serializable {

  val data: Array[Byte] = record

  override def hashCode: Int = new HashCodeBuilder().append(data).hashCode

  override def equals(other: Any): Boolean = {
    other match {
      case that: PegasusBytes => data.sameElements(that.data)
      case _                  => false
    }
  }
}

case class PegasusKey(key: Array[Byte]) extends PegasusBytes(record = key)
case class PegasusValue(value: Array[Byte]) extends PegasusBytes(record = value)
