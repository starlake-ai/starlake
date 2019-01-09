package com.ebiznext.comet.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

/**
  * Recognized file type format. This will select allow the correct parser
  * @param value : JSON of DSV
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[FormatDeserializer])
sealed case class Format(value: String) {
  override def toString: String = value
}

object Format {
  def fromString(value: String): Format = {
    value.toUpperCase match {
      case "DSV" => Format.DSV
      case "JSON" => Format.JSON
    }
  }

  object DSV extends Format("DSV")

  object JSON extends Format("JSON")

  val formats: Set[Format] = Set(DSV, JSON)
}

class FormatDeserializer extends JsonDeserializer[Format] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Format = {
    val value = jp.readValueAs[String](classOf[String])
    Format.fromString(value)
  }
}

