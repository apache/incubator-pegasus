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
  FilterType,
  MultiGetOptions,
  PegasusTableInterface,
  HashKeyData => PHashKeyData,
  SetItem => PSetItem
}
import org.apache.pegasus.scalaclient.{Serializer => SER}
import org.apache.commons.lang3.tuple.Pair

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
  * [Copyright]
  * Author: oujinliang
  * 3/15/18 10:34 PM
  */
private[scalaclient] trait PegasusUtil {
  implicit def convertOption(option: Options.MultiGet) = {
    val result = new MultiGetOptions()
    result.startInclusive = option.startInclusive
    result.stopInclusive = option.stopInclusive
    result.sortKeyFilterType = option.sortKeyFilterType
    result.sortKeyFilterPattern = option.sortKeyFilterPattern
    result.noValue = option.noValue
    result
  }

  implicit def ser[A](a: A)(implicit ser: SER[A]): Array[Byte] =
    ser.serialize(a)
  implicit def ser[A](list: Seq[A])(
      implicit ser: SER[A]): util.List[Array[Byte]] = {
    val javaList = new util.ArrayList[Array[Byte]](list.size)
    list.foreach { a =>
      javaList.add(ser.serialize(a))
    }
    javaList
  }

  implicit def deser[A](bytes: Array[Byte])(implicit ser: SER[A]): A =
    ser.deserialize(bytes)

  @inline implicit def durationToMs(timeout: Duration): Int =
    timeout.toMillis.toInt

  def pegasusKeysToPairList[H, S](keys: Seq[PegasusKey[H, S]])(
      implicit hSer: SER[H],
      sSer: SER[S]): util.List[Pair[Array[Byte], Array[Byte]]] = {
    keys.map(k => Pair.of(hSer.serialize(k.hashKey), sSer.serialize(k.sortKey)))
  }

  def pairToEither[A, B](pair: Pair[A, B]): Either[A, B] = {
    if (pair.getLeft != null) Left(pair.getLeft) else Right(pair.getRight)
  }

  def pairToEither[A, B, C](pair: Pair[A, B], f: B => C): Either[A, C] = {
    if (pair.getLeft != null) Left(pair.getLeft) else Right(f(pair.getRight))
  }

  def bytesPairToTuple[A, B](pair: Pair[Array[Byte], Array[Byte]])(
      implicit aSer: SER[A],
      bSer: SER[B]) = {
    (aSer.deserialize(pair.getLeft), bSer.deserialize(pair.getRight))
  }

  def tupleToBytesPair[A, B](t: (A, B))(
      implicit aSer: SER[A],
      bSer: SER[B]): Pair[Array[Byte], Array[Byte]] = {
    Pair.of(aSer.serialize(t._1), bSer.serialize(t._2))
  }

  def toListOfOption[A](list: java.util.List[A]): List[Option[A]] = {
    list.toList.map(Option.apply)
  }

  def convertMultiGetKeys[H, S](keys: Seq[(H, Seq[S])])(implicit hSer: SER[H],
                                                        sSer: SER[S])
    : java.util.List[Pair[Array[Byte], util.List[Array[Byte]]]] = {
    keys.map {
      case (hashKey, sortKeys) =>
        Pair.of(hSer.serialize(hashKey), ser(sortKeys))
    }
  }

  def convertToHashKeyData[H, S, V](data: PHashKeyData)(
      implicit hSer: SER[H],
      sSer: SER[S],
      vSer: SER[V]): HashKeyData[H, S, V] = {
    HashKeyData[H, S, V](hSer.deserialize(data.hashKey),
                         data.values.toList.map(bytesPairToTuple[S, V]))
  }

  def convertValue[S, V](value: (S, Array[Byte]))(implicit ser: SER[V]) =
    (value._1, ser.deserialize(value._2))

  def convertHashKeyData[H, S, V](data: HashKeyData[H, S, Array[Byte]])(
      implicit ser: SER[V]): HashKeyData[H, S, V] = {
    HashKeyData[H, S, V](data.hashKey, data.values.map(convertValue[S, V]))
  }

  def convertHashKeyData[H, S, V](data: HashKeyData[H, S, V])(
      implicit hSer: SER[H],
      sSer: SER[S],
      vSer: SER[V]): PHashKeyData = {
    new PHashKeyData(data.hashKey, data.values.map(tupleToBytesPair(_)))
  }

  def convertMultiGetResult[S](result: PegasusTableInterface.MultiGetResult)(
      implicit sSer: SER[S]) = {
    MultiGetResult(result.isAllFetched(), result.getValues().toList.map { p =>
      (sSer.deserialize(p.getLeft), p.getRight)
    })
  }

  def convertSetItem[H, S, V](item: SetItem[H, S, V])(implicit hSer: SER[H],
                                                      sSer: SER[S],
                                                      vSer: SER[V]) = {
    new PSetItem(item.hashKey,
                 item.sortKey,
                 item.value,
                 item.ttl.toSeconds.toInt)
  }

}

object PegasusUtil extends PegasusUtil
