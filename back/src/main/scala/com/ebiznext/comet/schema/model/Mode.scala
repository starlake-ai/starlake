package com.ebiznext.comet.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

/**
  * Big versus Fast data ingestion. Are we ingesting a file or a message stream ?
  * @param value : FILE or STREAM
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[ModeDeserializer])
sealed case class Mode(value: String) {
  override def toString: String = value
}

object Mode {

  def fromString(value: String): Mode = {
    value.toUpperCase() match {
      case "FILE"   => Mode.FILE
      case "STREAM" => Mode.STREAM
    }
  }

  object FILE extends Mode("FILE")

  object STREAM extends Mode("STREAM")

  val modes: Set[Mode] = Set(FILE, STREAM)
}

class ModeDeserializer extends JsonDeserializer[Mode] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Mode = {
    val value = jp.readValueAs[String](classOf[String])
    Mode.fromString(value)
  }
}
