package com.ebiznext.comet.utils

import java.io.{ Serializable => JSerializable }

import org.apache.commons.lang3.SerializationUtils

/**
 * Created by Mourad on 23/07/2018.
 */
object SerDeUtils {

  implicit def serialize[P](value: P): Array[Byte] = {
    val valueSerialized: Array[Byte] = SerializationUtils.serialize(value.asInstanceOf[JSerializable])
    valueSerialized
  }

  implicit def deserialize[P >: Null <: AnyRef](value: Array[Byte]): P = {
    Option(value) match {
      case Some(v) => SerializationUtils.deserialize(value)
      case None => null
    }
  }
}
