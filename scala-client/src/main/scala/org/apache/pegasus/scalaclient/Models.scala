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

import org.apache.pegasus.client.PException
import org.apache.pegasus.client.{FilterType, PException}
import scala.concurrent.duration._
import org.apache.pegasus.scalaclient.{Serializer => SER}

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
