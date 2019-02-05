package com.ebiznext.comet.schema.model

import com.ebiznext.comet.schema.model.WriteMode.{APPEND, ERROR_IF_EXISTS, IGNORE, OVERWRITE}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.spark.sql.SaveMode


/**
  * During ingestion, should the data be appended to the previous ones or should it replace the existing ones ?
  * see Spark SaveMode for more options.
  *
  * @param value : OVERWRITE / APPEND / ERROR_IF_EXISTS / IGNORE.
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[WriteDeserializer])
sealed case class WriteMode(value: String) {
  override def toString: String = value

  def toSaveMode: SaveMode = {
    this match {
      case OVERWRITE => SaveMode.Overwrite
      case APPEND => SaveMode.Append
      case ERROR_IF_EXISTS => SaveMode.ErrorIfExists
      case IGNORE => SaveMode.Ignore
      case _ =>
        throw new Exception("Should never happen")
    }
  }
}

object WriteMode {
  def fromString(value: String): WriteMode = {
    value.toUpperCase() match {
      case "OVERWRITE" => WriteMode.OVERWRITE
      case "APPEND" => WriteMode.APPEND
      case "ERROR_IF_EXISTS" => WriteMode.ERROR_IF_EXISTS
      case "IGNORE" => WriteMode.IGNORE
    }
  }

  object OVERWRITE extends WriteMode("OVERWRITE")

  object APPEND extends WriteMode("APPEND")

  object ERROR_IF_EXISTS extends WriteMode("ERROR_IF_EXISTS")

  object IGNORE extends WriteMode("IGNORE")

  val writes: Set[WriteMode] = Set(OVERWRITE, APPEND, ERROR_IF_EXISTS, IGNORE)
}

class WriteDeserializer extends JsonDeserializer[WriteMode] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): WriteMode = {
    val value = jp.readValueAs[String](classOf[String])
    WriteMode.fromString(value)
  }
}

