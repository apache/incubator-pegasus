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

package org.apache.pegasus

import org.apache.pegasus.client.{FilterType, PException}
import scala.concurrent.duration._
import org.apache.pegasus.scalaclient.{Serializer => SER}

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
