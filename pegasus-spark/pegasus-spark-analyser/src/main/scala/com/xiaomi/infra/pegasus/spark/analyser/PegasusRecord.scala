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

package com.xiaomi.infra.pegasus.spark.analyser

import java.nio.ByteBuffer
import java.util

import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.commons.lang3.Validate
import org.apache.commons.lang3.tuple.ImmutablePair
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

  def restoreValue(value: Array[Byte]): Array[Byte]

  def getPegasusRecord(rocksIterator: RocksIterator): PegasusRecord = {
    val keyPair = restoreKey(rocksIterator.key)
    PegasusRecord(
      keyPair.getLeft,
      keyPair.getRight,
      restoreValue(rocksIterator.value),
      restoreExpireTs(rocksIterator.value)
    )
  }
}

class DataVersion1 extends DataVersion {

  def restoreValue(value: Array[Byte]): Array[Byte] =
    util.Arrays.copyOfRange(value, 4, value.length)

}

class DataVersion2 extends DataVersion {

  def restoreValue(value: Array[Byte]): Array[Byte] =
    util.Arrays.copyOfRange(value, 12, value.length)

}

case class PegasusRecord(
    hashKey: Array[Byte],
    sortKey: Array[Byte],
    value: Array[Byte],
    expireTs: Int
) {
  override def toString: String =
    String.format(
      "[HashKey=%s, SortKey=%s, Value=%s, ExpireTs=%s]",
      util.Arrays.toString(hashKey),
      util.Arrays.toString(sortKey),
      util.Arrays.toString(value),
      String.valueOf(expireTs)
    )

  override def equals(other: Any): Boolean = {
    other match {
      case that: PegasusRecord =>
        (that canEqual this) &&
          hashKey.sameElements(that.hashKey) &&
          sortKey.sameElements(that.sortKey) &&
          value.sameElements(that.value) &&
          expireTs == that.expireTs
      case _ => false
    }
  }

  override def hashCode: Int = {
    new HashCodeBuilder()
      .append(hashKey)
      .append(sortKey)
      .append(value)
      .append(expireTs)
      .hashCode()
  }
}
