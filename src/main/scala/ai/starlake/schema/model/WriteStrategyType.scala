package ai.starlake.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[StrategyNameDeserializer])
sealed case class WriteStrategyType(value: String) {
  override def toString: String = value
  def toWriteMode(): WriteMode = {
    this match {
      case WriteStrategyType.OVERWRITE                   => WriteMode.OVERWRITE
      case WriteStrategyType.APPEND                      => WriteMode.APPEND
      case WriteStrategyType.UPSERT_BY_KEY               => WriteMode.APPEND
      case WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP => WriteMode.APPEND
      case WriteStrategyType.SCD2                        => WriteMode.APPEND
      case WriteStrategyType.OVERWRITE_BY_PARTITION      => WriteMode.APPEND
      case _ =>
        throw new Exception("Should never happen")
    }

  }

  def requireKey(): Boolean = {
    this match {
      case WriteStrategyType.UPSERT_BY_KEY               => true
      case WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP => true
      case WriteStrategyType.SCD2                        => true
      case _                                             => false
    }
  }

  def requireTimestamp(): Boolean = {
    this match {
      case WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP => true
      case WriteStrategyType.SCD2                        => true
      case _                                             => false
    }
  }
}

object WriteStrategyType {
  def fromWriteMode(mode: WriteMode): WriteStrategyType = fromString(mode.value)

  def fromString(value: String): WriteStrategyType = {
    value.toUpperCase match {
      case "OVERWRITE"                   => WriteStrategyType.OVERWRITE
      case "APPEND"                      => WriteStrategyType.APPEND
      case "UPSERT_BY_KEY"               => WriteStrategyType.UPSERT_BY_KEY
      case "UPSERT_BY_KEY_AND_TIMESTAMP" => WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP
      case "SCD2"                        => WriteStrategyType.SCD2
      case "OVERWRITE_BY_PARTITION"      => WriteStrategyType.OVERWRITE_BY_PARTITION
      case _                             => WriteStrategyType(value)

    }
  }

  object APPEND extends WriteStrategyType("APPEND")

  object OVERWRITE extends WriteStrategyType("OVERWRITE")

  object UPSERT_BY_KEY extends WriteStrategyType("UPSERT_BY_KEY")

  object UPSERT_BY_KEY_AND_TIMESTAMP extends WriteStrategyType("UPSERT_BY_KEY_AND_TIMESTAMP")

  object OVERWRITE_BY_PARTITION extends WriteStrategyType("OVERWRITE_BY_PARTITION")

  object SCD2 extends WriteStrategyType("SCD2")

  val strategies: Set[WriteStrategyType] =
    Set(APPEND, OVERWRITE, UPSERT_BY_KEY, UPSERT_BY_KEY_AND_TIMESTAMP, SCD2, OVERWRITE_BY_PARTITION)
}

class StrategyNameDeserializer extends JsonDeserializer[WriteStrategyType] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): WriteStrategyType = {
    val value = jp.readValueAs[String](classOf[String])
    WriteStrategyType.fromString(value)
  }
}
