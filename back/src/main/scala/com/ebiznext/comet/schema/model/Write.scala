package com.ebiznext.comet.schema.model

import com.ebiznext.comet.schema.model.Write.{APPEND, ERROR_IF_EXISTS, IGNORE, OVERWRITE}
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
sealed case class Write(value: String) {
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

object Write {
  def fromString(value: String): Write = {
    value.toUpperCase() match {
      case "OVERWRITE" => Write.OVERWRITE
      case "APPEND" => Write.APPEND
      case "ERROR_IF_EXISTS" => Write.ERROR_IF_EXISTS
      case "IGNORE" => Write.IGNORE
    }
  }

  object OVERWRITE extends Write("OVERWRITE")

  object APPEND extends Write("APPEND")

  object ERROR_IF_EXISTS extends Write("ERROR_IF_EXISTS")

  object IGNORE extends Write("IGNORE")

  val writes: Set[Write] = Set(OVERWRITE, APPEND, ERROR_IF_EXISTS, IGNORE)
}

class WriteDeserializer extends JsonDeserializer[Write] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Write = {
    val value = jp.readValueAs[String](classOf[String])
    Write.fromString(value)
  }
}

