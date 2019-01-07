package com.ebiznext.comet.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[PrivacyLevelDeserializer])
sealed case class PrivacyLevel(value: String) {
  override def toString: String = value
}

object PrivacyLevel {
  def fromString(value: String): PrivacyLevel = {
    value.toUpperCase() match {
      case "NONE" => PrivacyLevel.NONE
      case "HIDE" => PrivacyLevel.HIDE
      case "MD5" => PrivacyLevel.MD5
      case "SHA1" => PrivacyLevel.SHA1
      case "SHA256" => PrivacyLevel.SHA256
      case "SHA512" => PrivacyLevel.SHA512
      case "AES" => PrivacyLevel.AES
    }
  }

  object NONE extends PrivacyLevel("NONE")

  object HIDE extends PrivacyLevel("HIDE")

  object MD5 extends PrivacyLevel("MD5")

  object SHA1 extends PrivacyLevel("SHA1")

  object SHA256 extends PrivacyLevel("SHA256")

  object SHA512 extends PrivacyLevel("SHA512")

  object AES extends PrivacyLevel("AES")

  val privacyLevels: Set[PrivacyLevel] = Set(NONE, HIDE, MD5, SHA1, SHA256, SHA512, AES)
}

class PrivacyLevelDeserializer extends JsonDeserializer[PrivacyLevel] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): PrivacyLevel = {
    val value = jp.readValueAs[String](classOf[String])
    PrivacyLevel.fromString(value)
  }
}

