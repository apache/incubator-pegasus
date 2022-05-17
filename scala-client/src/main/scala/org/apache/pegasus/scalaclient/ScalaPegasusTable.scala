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

package org.apache.pegasus.scalaclient

import java.util

import org.apache.pegasus.client.{
  PException,
  PegasusTableInterface => ITable,
  HashKeyData => PHashKeyData
}
import org.apache.pegasus.scalaclient.{Serializer => SER}
import org.apache.commons.lang3.tuple.Pair

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
  * [Copyright]
  * Author: oujinliang
  * 3/15/18 9:20 PM
  */
trait ScalaPegasusTable extends PegasusUtil {
  private[scalaclient] val table: ITable

  import Serializers._

  @throws[PException]
  def exists[H, S](hashKey: H, sortKey: S, timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]): Boolean = {
    table.exist(hashKey, sortKey, timeout)
  }

  @throws[PException]
  def sortKeyCount[H](hashKey: H, timeout: Duration = 0 milli)(
      implicit hSer: SER[H]) = {
    table.sortKeyCount(hashKey, timeout)
  }

  @throws[PException]
  def get[H, S](hashKey: H, sortKey: S, timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]): PegasusResult = {
    val result = table.get(hashKey, sortKey, timeout)
    PegasusResult(result)
  }

  @throws[PException]
  def batchGet[H, S](keys: Seq[PegasusKey[H, S]], timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]): PegasusResultList = {
    val result = new util.ArrayList[Array[Byte]]()
    table.batchGet(pegasusKeysToPairList(keys), result, timeout)
    PegasusResultList(result.toList)
  }

  @throws[PException]
  def batchGet2[H, S](keys: Seq[PegasusKey[H, S]], timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]): BatchGetResult[Array[Byte]] = {
    val result = new util.ArrayList[Pair[PException, Array[Byte]]]()
    val count = table.batchGet2(pegasusKeysToPairList(keys), result, timeout)

    BatchGetResult(count,
                   result.toList.map(pairToEither[PException, Array[Byte]]))
  }

  @throws[PException]
  def multiGet[H, S](hashKey: H,
                     sortKeys: Seq[S],
                     maxFetchCount: Int = 100,
                     maxFetchSize: Int = 1000000,
                     timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]): MultiGetResult[S, Array[Byte]] = {
    val result =
      table.multiGet(hashKey, sortKeys, maxFetchCount, maxFetchSize, timeout)
    convertMultiGetResult(result)
  }

  @throws[PException]
  def multiGetRange[H, S](hashKey: H,
                          startSortKey: S,
                          stopSortKey: S,
                          options: Options.MultiGet,
                          maxFetchCount: Int = 100,
                          maxFetchSize: Int = 1000000,
                          timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]): MultiGetResult[S, Array[Byte]] = {
    val result = table.multiGet(hashKey,
                                startSortKey,
                                stopSortKey,
                                options,
                                maxFetchCount,
                                maxFetchSize,
                                timeout)
    convertMultiGetResult(result)
  }

  @throws[PException]
  def batchMultiGet[H, S](keys: Seq[(H, Seq[S])], timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]): List[HashKeyData[H, S, Array[Byte]]] = {
    val result = new util.ArrayList[PHashKeyData]()
    table.batchMultiGet(convertMultiGetKeys(keys), result, timeout)

    result.map(convertToHashKeyData[H, S, Array[Byte]]).toList
  }

  @throws[PException]
  def batchMultiGet2[H, S](keys: Seq[(H, Seq[S])], timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]): BatchMultiGetResult[H, S, Array[Byte]] = {
    val result = new util.ArrayList[Pair[PException, PHashKeyData]]()
    val count = table.batchMultiGet2(convertMultiGetKeys(keys), result, timeout)

    val either = result
      .map(p => pairToEither(p, convertToHashKeyData[H, S, Array[Byte]]))
      .toList
    BatchMultiGetResult(count, either)
  }

  @throws[PException]
  def multiGetSortKeys[H](
      hashKey: H,
      maxFetchCount: Int = 100,
      maxFetchSize: Int = 1000000,
      timeout: Duration = 0 milli)(implicit hSer: SER[H]) = {
    val result =
      table.multiGetSortKeys(hashKey, maxFetchCount, maxFetchSize, timeout)

    MultiGetSortKeysResult(result.allFetched, result.keys.toList)
  }

  @throws[PException]
  def set[H, S, V](hashKey: H,
                   sortKey: S,
                   value: V,
                   ttl: Duration = 0 second,
                   timeout: Duration = 0 milli)(implicit hSer: SER[H],
                                                sSer: SER[S],
                                                vSer: SER[V]) = {
    table.set(hashKey, sortKey, value, ttl.toSeconds.toInt, timeout)
  }

  @throws[PException]
  def batchSet[H, S, V](items: Seq[SetItem[H, S, V]],
                        timeout: Duration = 0 milli)(implicit hSer: SER[H],
                                                     sSer: SER[S],
                                                     vSer: SER[V]) = {
    table.batchSet(items.map(convertSetItem[H, S, V]), timeout)
  }

  @throws[PException]
  def batchSet2[H, S, V](items: Seq[SetItem[H, S, V]],
                         timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S],
      vSer: SER[V]): BatchOpResult = {
    val result = new util.ArrayList[PException](items.size)
    val count = table.batchSet2(items.map(convertSetItem(_)), result, timeout)
    BatchOpResult(count, result.toList.map(Option.apply))
  }

  @throws[PException]
  def multiSet[H, S, V](hashKey: H,
                        values: Seq[(S, V)],
                        ttl: Duration = 0 second,
                        timeout: Duration = 0 milli)(implicit hSer: SER[H],
                                                     sSer: SER[S],
                                                     vSer: SER[V]) = {
    table.multiSet(hashKey,
                   values.map(tupleToBytesPair(_)),
                   ttl.toSeconds.toInt,
                   timeout)
  }

  @throws[PException]
  def batchMultiSet[H, S, V](items: Seq[HashKeyData[H, S, V]],
                             ttl: Duration = 0 second,
                             timeout: Duration = 0 milli)(implicit hSer: SER[H],
                                                          sSer: SER[S],
                                                          vSer: SER[V]) = {
    table.batchMultiSet(items.map(convertHashKeyData[H, S, V]),
                        ttl.toSeconds.toInt,
                        timeout)
  }

  @throws[PException]
  def batchMultiSet2[H, S, V](items: Seq[HashKeyData[H, S, V]],
                              ttl: Duration = 0 second,
                              timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S],
      vSer: SER[V]): BatchOpResult = {
    val result = new util.ArrayList[PException](items.size)
    val count = table.batchMultiSet2(items.map(convertHashKeyData[H, S, V]),
                                     ttl.toSeconds.toInt,
                                     result,
                                     timeout)
    BatchOpResult(count, result.toList.map(Option.apply))
  }

  @throws[PException]
  def del[H, S](hashKey: H, sortKey: S, timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    table.del(hashKey, sortKey, timeout)
  }

  @throws[PException]
  def batchDel[H, S](keys: Seq[PegasusKey[H, S]], timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    table.batchDel(pegasusKeysToPairList(keys), timeout)
  }

  @throws[PException]
  def batchDel2[H, S](keys: Seq[PegasusKey[H, S]], timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    val result = new util.ArrayList[PException](keys.size)
    val count = table.batchDel2(pegasusKeysToPairList(keys), result, timeout)
    BatchOpResult(count, result.toList.map(Option.apply))
  }

  @throws[PException]
  def multiDel[H, S](hashKey: H, sortKeys: Seq[S], timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    table.multiDel(hashKey, sortKeys, timeout)
  }

  @throws[PException]
  def batchMultiDel[H, S](keys: Seq[(H, Seq[S])], timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    table.batchMultiDel(convertMultiGetKeys(keys), timeout)
  }

  @throws[PException]
  def batchMultiDel2[H, S](keys: Seq[(H, Seq[S])], timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    val result = new util.ArrayList[PException](keys.size)
    val count = table.batchMultiDel2(convertMultiGetKeys(keys), result, timeout)
    BatchOpResult(count, result.toList.map(Option.apply))
  }

  @throws[PException]
  def ttl[H, S](hashKey: H, sortKey: S, timeout: Duration = 0 milli)(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    table.ttl(hashKey, sortKey, timeout)
  }

  @throws[PException]
  def incr[H, S](
      hashKey: H,
      sortKey: S,
      increment: Long,
      ttl: Duration = 0 milli,
      timeout: Duration = 0 milli)(implicit hSer: SER[H], sSer: SER[S]) = {
    table.incr(hashKey,
               sortKey,
               increment,
               ttl.toSeconds.toInt,
               timeout.toMillis.toInt)
  }
}

class ScalaPegasusTableImpl(val table: ITable) extends ScalaPegasusTable
