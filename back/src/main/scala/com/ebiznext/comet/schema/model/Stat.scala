package com.ebiznext.comet.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

/**
  * This attribute property let us know what statistics should be computed for this field
  * when analyze is active.
  * @param value : DISCRETE or CONTINUOUS or TEXT
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[ModeDeserializer])
sealed case class Stat(value: String) {
  override def toString: String = value
}

object Stat {
  def fromString(value: String): Stat = {
    value.toUpperCase() match {
      case "DISCRETE" => Stat.DISCRETE
      case "CONTINUOUS" => Stat.CONTINUOUS
      case "TEXT" => Stat.TEXT
      case "NONE" => Stat.NONE
    }
  }

  object DISCRETE extends Stat("DISCRETE")

  object CONTINUOUS extends Stat("CONTINUOUS")

  object TEXT extends Stat("TEXT")

  object NONE extends Stat("NONE")

  val stats: Set[Stat] = Set(NONE, DISCRETE, CONTINUOUS, TEXT)
}

class StatDeserializer extends JsonDeserializer[Stat] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Stat = {
    val value = jp.readValueAs[String](classOf[String])
    Stat.fromString(value)
  }
}
