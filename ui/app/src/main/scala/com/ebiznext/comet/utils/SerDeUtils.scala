package com.ebiznext.comet.utils

import org.apache.commons.lang3.SerializationUtils
import java.io.{ Serializable => JSerializable }

/**
 * Created by Mourad on 23/07/2018.
 */
object SerDeUtils {

  def serialize(value: Any): Array[Byte] = {
    val valueSerialized: Array[Byte] = SerializationUtils.serialize(value.asInstanceOf[JSerializable])
    valueSerialized
  }

  def deserialize(value: Array[Byte]): Any = {
    val valueDeserialized: Any = SerializationUtils.deserialize(value)
    valueDeserialized
  }
}
