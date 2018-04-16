package com.xiaomi.infra.pegasus.scalaclient

import org.apache.commons.lang3.SerializationUtils

trait AnyRefSerializer {

    implicit def anyRefSerializer[T <: java.io.Serializable] = new Serializer[T] {
        override def serialize(obj: T): Array[Byte] = if (obj == null) null else SerializationUtils.serialize(obj)
        override def deserialize(bytes: Array[Byte]): T = if (bytes == null) null.asInstanceOf[T] else  SerializationUtils.deserialize(bytes).asInstanceOf[T]
    }
}
