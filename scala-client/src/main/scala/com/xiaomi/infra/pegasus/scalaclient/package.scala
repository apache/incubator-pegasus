package com.xiaomi.infra.pegasus

import com.xiaomi.infra.pegasus.client.{FilterType, PException}
import com.xiaomi.infra.pegasus.scalaclient.{Serializer => SER}
import scala.concurrent.duration._

/**
  * [Copyright]
  * Author: oujinliang
  * 3/22/18 9:33 PM
  */
package object scalaclient extends PegasusUtil {

  def as[V](implicit s: SER[V]) = s

  implicit class BatchGetWrap(result: BatchGetResult[Array[Byte]]) {
    def as[V](implicit ser: SER[V]): BatchGetResult[V] = {
      BatchGetResult[V](result.count,
                        result.result.map(_.right.map(ser.deserialize)))
    }
  }

  implicit class BatchMultiGetWrap[H, S](
      result: BatchMultiGetResult[H, S, Array[Byte]]) {
    def as[V](implicit ser: SER[V]): BatchMultiGetResult[H, S, V] = {
      BatchMultiGetResult(
        result.count,
        result.values.map(_.right.map(convertHashKeyData[H, S, V])))
    }
  }

  implicit class MulitGetWrap[S](result: MultiGetResult[S, Array[Byte]]) {
    def as[V](implicit ser: SER[V]): MultiGetResult[S, V] = {
      MultiGetResult(result.allFetched, result.values.map(convertValue[S, V]))
    }
  }

  implicit class MultiGetSortKeysWrap(
      result: MultiGetSortKeysResult[Array[Byte]]) {
    def as[S](implicit ser: SER[S]): MultiGetSortKeysResult[S] = {
      MultiGetSortKeysResult(result.allFetched,
                             result.values.map(ser.deserialize))
    }
  }

}
