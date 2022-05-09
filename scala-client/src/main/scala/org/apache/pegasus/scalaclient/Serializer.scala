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

import com.google.common.primitives._
import org.apache.commons.lang3.SerializationUtils

/**
  * [Copyright]
  * Author: oujinliang
  * 3/15/18 9:30 PM
  */
trait Serializer[T] {
  def serialize(obj: T): Array[Byte]
  def deserialize(bytes: Array[Byte]): T
}

/** Default Serializers */
object Serializers {

  implicit object Raw extends Serializer[Array[Byte]] {
    @inline override def serialize(obj: Array[Byte]): Array[Byte] = obj
    @inline override def deserialize(bytes: Array[Byte]): Array[Byte] = bytes
  }

  implicit object Utf8String extends Serializer[String] {
    override def serialize(obj: String): Array[Byte] =
      if (obj == null) null else obj.getBytes("UTF-8")
    override def deserialize(bytes: Array[Byte]): String =
      if (bytes == null) null else new String(bytes, "UTF-8")
  }

  implicit object BooleanSerializer extends Serializer[Boolean] {
    override def serialize(obj: Boolean): Array[Byte] =
      Array((if (obj) 1 else 0).asInstanceOf[Byte])
    override def deserialize(bytes: Array[Byte]): Boolean = {
      require(bytes.length == 1, "not a boolean")
      bytes(0) == 1.asInstanceOf[Byte]
    }
  }

  implicit object IntSerializer extends Serializer[Int] {
    override def serialize(obj: Int): Array[Byte] =
      Ints.toByteArray(obj) // TODO: remove guava.
    override def deserialize(bytes: Array[Byte]): Int =
      Ints.fromByteArray(bytes)
  }

  implicit object LongSerializer extends Serializer[Long] {
    override def serialize(obj: Long): Array[Byte] = Longs.toByteArray(obj)
    override def deserialize(bytes: Array[Byte]): Long =
      Longs.fromByteArray(bytes)
  }

  implicit object ShortSerializer extends Serializer[Short] {
    override def serialize(obj: Short): Array[Byte] = Shorts.toByteArray(obj)
    override def deserialize(bytes: Array[Byte]): Short =
      Shorts.fromByteArray(bytes)
  }

  implicit object DoubleSerializer extends Serializer[Double] {
    override def serialize(obj: Double): Array[Byte] =
      Longs.toByteArray(java.lang.Double.doubleToLongBits(obj))
    override def deserialize(bytes: Array[Byte]): Double =
      java.lang.Double.longBitsToDouble(Longs.fromByteArray(bytes))
  }

  def anyRefSerializer[T <: java.io.Serializable] = new Serializer[T] {
    override def serialize(obj: T): Array[Byte] =
      if (obj == null) null else SerializationUtils.serialize(obj)
    override def deserialize(bytes: Array[Byte]): T =
      if (bytes == null) null.asInstanceOf[T]
      else SerializationUtils.deserialize(bytes).asInstanceOf[T]
  }
}

object AnyRefSerializer {
  implicit def andRefSerializer[T <: java.io.Serializable] =
    Serializers.anyRefSerializer[T]
}
