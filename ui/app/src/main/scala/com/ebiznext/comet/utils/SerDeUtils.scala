package com.ebiznext.comet.utils

import java.io.{Serializable => JSerializable}

import org.apache.commons.lang3.SerializationUtils

/**
  * Created by Mourad on 23/07/2018.
  */
object SerDeUtils extends JsonSupport {

  implicit def serialize[P <: AnyRef](value: P): Array[Byte] = {
    val str: String                  = toJSON(value)
    val valueSerialized: Array[Byte] = SerializationUtils.serialize(str.asInstanceOf[JSerializable])
    valueSerialized
  }

  implicit def deserialize[P <: AnyRef](value: Array[Byte])(implicit m: Manifest[P]): Option[P] = {
    Option(value) match {
      case Some(v) => {
        val json: String      = SerializationUtils.deserialize(v)
        val option: Option[P] = fromJSON[P](json)
        option
      }
      case _ => Option.empty[P]
    }
  }
}
