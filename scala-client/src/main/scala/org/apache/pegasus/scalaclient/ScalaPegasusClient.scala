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

import java.util.Properties
import org.apache.pegasus.client.{
  PException,
  TableOptions,
  PegasusClientInterface => IClient
}
import org.apache.pegasus.scalaclient.{Serializer => SER}

import scala.concurrent.duration._

/**
  * [Copyright]
  * Author: oujinliang
  * 3/15/18 9:07 PM
  */
trait ScalaPegasusClient extends PegasusUtil {
  val client: IClient

  implicit val raw = Serializers.Raw

  def close = client.close()

  def getConfiguration = client.getConfiguration

  @throws[PException]
  def openTable(table: String) =
    new ScalaPegasusTableImpl(client.openTable(table))

  @throws[PException]
  def openTable(table: String, options: TableOptions) =
    new ScalaPegasusTableImpl(client.openTable(table, options))

  @throws[PException]
  def openAsyncTable(table: String) =
    new ScalaPegasusAsyncTableImpl(client.openTable(table))

  @throws[PException]
  def openAsyncTable(table: String, options: TableOptions) =
    new ScalaPegasusAsyncTableImpl(client.openTable(table, options))

  private def getTable(table: String) = openTable(table)

  @throws[PException]
  def exists[H, S](table: String, hashKey: H, sortKey: S)(
      implicit hSer: SER[H],
      sSer: SER[S]): Boolean = {
    getTable(table).exists(hashKey, sortKey)
  }

  @throws[PException]
  def sortKeyCount[H](table: String, hashKey: H)(implicit hSer: SER[H]) =
    getTable(table).sortKeyCount(hashKey)

  @throws[PException]
  def get[H, S](table: String, hashKey: H, sortKey: S)(implicit hSer: SER[H],
                                                       sSer: SER[S]) = {
    getTable(table).get(hashKey, sortKey)
  }

  @throws[PException]
  def batchGet[H, S](table: String, keys: List[PegasusKey[H, S]])(
      implicit hSer: SER[H],
      sSer: SER[S]): PegasusResultList = {
    getTable(table).batchGet(keys)
  }

  @throws[PException]
  def batchGet2[H, S](table: String, keys: Seq[PegasusKey[H, S]])(
      implicit hSer: SER[H],
      sSer: SER[S]): BatchGetResult[Array[Byte]] = {
    getTable(table).batchGet2(keys)
  }

  @throws[PException]
  def multiGet[H, S](table: String,
                     hashKey: H,
                     sortKeys: Seq[S],
                     maxFetchCount: Int = 100,
                     maxFetchSize: Int = 1000000)(
      implicit hSer: SER[H],
      sSer: SER[S]): MultiGetResult[S, Array[Byte]] = {
    getTable(table).multiGet(hashKey, sortKeys, maxFetchCount, maxFetchSize)
  }

  @throws[PException]
  def multiGetRange[H, S](table: String,
                          hashKey: H,
                          startSortKey: S,
                          stopSortKey: S,
                          options: Options.MultiGet,
                          maxFetchCount: Int = 100,
                          maxFetchSize: Int = 1000000)(
      implicit hSer: SER[H],
      sSer: SER[S]): MultiGetResult[S, Array[Byte]] = {
    getTable(table).multiGetRange(hashKey,
                                  startSortKey,
                                  stopSortKey,
                                  options,
                                  maxFetchCount,
                                  maxFetchSize)
  }

  @throws[PException]
  def batchMultiGet[H, S](table: String, keys: Seq[(H, Seq[S])])(
      implicit hSer: SER[H],
      sSer: SER[S]): List[HashKeyData[H, S, Array[Byte]]] = {
    getTable(table).batchMultiGet(keys)
  }

  @throws[PException]
  def batchMultiGet2[H, S](table: String, keys: Seq[(H, Seq[S])])(
      implicit hSer: SER[H],
      sSer: SER[S]): BatchMultiGetResult[H, S, Array[Byte]] = {
    getTable(table).batchMultiGet2(keys)
  }

  @throws[PException]
  def multiGetSortKeys[H](
      table: String,
      hashKey: H,
      maxFetchCount: Int = 100,
      maxFetchSize: Int = 1000000)(implicit hSer: SER[H]) = {
    getTable(table).multiGetSortKeys(hashKey, maxFetchCount, maxFetchSize)
  }

  @throws[PException]
  def set[H, S, V](table: String,
                   hashKey: H,
                   sortKey: S,
                   value: V,
                   ttl: Duration = 0 second)(implicit hSer: SER[H],
                                             sSer: SER[S],
                                             vSer: SER[V]) = {
    getTable(table).set(hashKey, sortKey, value, ttl)
  }

  @throws[PException]
  def batchSet[H, S, V](table: String, items: Seq[SetItem[H, S, V]])(
      implicit hSer: SER[H],
      sSer: SER[S],
      vSer: SER[V]) = {
    getTable(table).batchSet(items)
  }

  @throws[PException]
  def batchSet2[H, S, V](table: String, items: Seq[SetItem[H, S, V]])(
      implicit hSer: SER[H],
      sSer: SER[S],
      vSer: SER[V]): BatchOpResult = {
    getTable(table).batchSet2(items)
  }

  @throws[PException]
  def multiSet[H, S, V](table: String,
                        hashKey: H,
                        values: Seq[(S, V)],
                        ttl: Duration = 0 second)(implicit hSer: SER[H],
                                                  sSer: SER[S],
                                                  vSer: SER[V]) = {
    getTable(table).multiSet(hashKey, values, ttl)
  }

  @throws[PException]
  def batchMultitSet[H, S, V](table: String,
                              items: Seq[HashKeyData[H, S, V]],
                              ttl: Duration = 0 second)(implicit hSer: SER[H],
                                                        sSer: SER[S],
                                                        vSer: SER[V]) = {
    getTable(table).batchMultiSet(items, ttl)
  }

  @throws[PException]
  def batchMultiSet2[H, S, V](table: String,
                              items: Seq[HashKeyData[H, S, V]],
                              ttl: Duration = 0 second)(
      implicit hSer: SER[H],
      sSer: SER[S],
      vSer: SER[V]): BatchOpResult = {
    getTable(table).batchMultiSet2(items, ttl)
  }

  @throws[PException]
  def del[H, S](table: String, hashKey: H, sortKey: S)(implicit hSer: SER[H],
                                                       sSer: SER[S]) = {
    getTable(table).del(hashKey, sortKey)
  }

  @throws[PException]
  def batchDel[H, S](table: String, keys: Seq[PegasusKey[H, S]])(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    getTable(table).batchDel(keys)
  }

  @throws[PException]
  def batchDel2[H, S](table: String, keys: Seq[PegasusKey[H, S]])(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    getTable(table).batchDel2(keys)
  }

  @throws[PException]
  def multiDel[H, S](table: String, hashKey: H, sortKeys: Seq[S])(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    getTable(table).multiDel(hashKey, sortKeys)
  }

  @throws[PException]
  def batchMultiDel[H, S](table: String, keys: Seq[(H, Seq[S])])(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    getTable(table).batchMultiDel(keys)
  }

  @throws[PException]
  def batchMultiDel2[H, S](table: String, keys: Seq[(H, Seq[S])])(
      implicit hSer: SER[H],
      sSer: SER[S]) = {
    getTable(table).batchMultiDel2(keys)
  }

  @throws[PException]
  def ttl[H, S](table: String, hashKey: H, sortKey: S)(implicit hSer: SER[H],
                                                       sSer: SER[S]) = {
    getTable(table).ttl(hashKey, sortKey)
  }

  @throws[PException]
  def incr[H, S](
      table: String,
      hashKey: H,
      sortKey: S,
      increment: Long,
      ttl: Duration = 0 milli)(implicit hSer: SER[H], sSer: SER[S]) = {
    getTable(table).incr(hashKey, sortKey, increment, ttl)
  }
}

class ScalaPegasusClientImpl(val client: IClient) extends ScalaPegasusClient
