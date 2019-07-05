package com.xiaomi.infra.pegasus.scalaclient

import com.xiaomi.infra.pegasus.client.PException
import com.xiaomi.infra.pegasus.client.{FilterType, PException}
import com.xiaomi.infra.pegasus.scalaclient.{Serializer => SER}
import scala.concurrent.duration._

/**
  * [Copyright]
  * Author: oujinliang
  * 3/27/18 8:29 PM
  */
case class PegasusResult(bytes: Array[Byte]) {
  def as[V](implicit ser: SER[V]): V = ser.deserialize(bytes)
  def asOpt[V](implicit ser: SER[V]): Option[V] =
    if (isNull) None else Some(ser.deserialize(bytes))
  def isNull = bytes == null
}

case class PegasusResultList(list: List[Array[Byte]]) {
  def as[V](implicit ser: SER[V]): List[V] = list.map(ser.deserialize)
  def asOpt[V](implicit ser: SER[V]): List[Option[V]] = list.map { v =>
    if (v == null) None else Some(ser.deserialize(v))
  }
}

case class PegasusKey[H, S](hashKey: H, sortKey: S)

case class BatchGetResult[V](count: Int, result: List[Either[PException, V]])

case class BatchMultiGetResult[H, S, V](
    count: Int,
    values: List[Either[PException, HashKeyData[H, S, V]]])

case class MultiGetResult[S, V](allFetched: Boolean, values: List[(S, V)])

case class HashKeyData[H, S, V](hashKey: H, values: List[(S, V)])

case class SetItem[H, S, V](hashKey: H, sortKey: S, value: V, ttl: Duration)

case class BatchOpResult(count: Int, errors: List[Option[PException]])

case class MultiGetSortKeysResult[S](allFetched: Boolean, values: List[S])

object Options {

  case class MultiGet(
      startInclusive: Boolean = true,
      stopInclusive: Boolean = false, // if the stopSortKey is included
      sortKeyFilterType: FilterType = FilterType.FT_NO_FILTER, // filter type for sort key
      sortKeyFilterPattern: Array[Byte] = null, // filter pattern for sort key
      noValue: Boolean = false // only fetch hash_key and sort_key, but not fetch value
  )
}
