package ai.starlake.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[StrategyNameDeserializer])
sealed case class StrategyType(value: String) {
  override def toString: String = value
  def toWriteMode(): WriteMode = {
    this match {
      case StrategyType.OVERWRITE                   => WriteMode.OVERWRITE
      case StrategyType.APPEND                      => WriteMode.APPEND
      case StrategyType.UPSERT_BY_KEY               => WriteMode.APPEND
      case StrategyType.UPSERT_BY_KEY_AND_TIMESTAMP => WriteMode.APPEND
      case StrategyType.SCD2                        => WriteMode.APPEND
      case StrategyType.OVERWRITE_BY_PARTITION      => WriteMode.APPEND
      case _ =>
        throw new Exception("Should never happen")
    }

  }
}

object StrategyType {

  def fromString(value: String): StrategyType = {
    value.toUpperCase match {
      case "OVERWRITE"                   => StrategyType.OVERWRITE
      case "APPEND"                      => StrategyType.APPEND
      case "UPSERT_BY_KEY"               => StrategyType.UPSERT_BY_KEY
      case "UPSERT_BY_KEY_AND_TIMESTAMP" => StrategyType.UPSERT_BY_KEY_AND_TIMESTAMP
      case "SCD2"                        => StrategyType.SCD2
      case "OVERWRITE_BY_PARTITION"      => StrategyType.OVERWRITE_BY_PARTITION
      case _                             => StrategyType(value)

    }
  }

  object APPEND extends StrategyType("APPEND")

  object OVERWRITE extends StrategyType("OVERWRITE")

  object UPSERT_BY_KEY extends StrategyType("UPSERT_BY_KEY")

  object UPSERT_BY_KEY_AND_TIMESTAMP extends StrategyType("UPSERT_BY_KEY_AND_TIMESTAMP")

  object OVERWRITE_BY_PARTITION extends StrategyType("OVERWRITE_BY_PARTITION")

  object SCD2 extends StrategyType("SCD2")

  val strategies: Set[StrategyType] =
    Set(APPEND, OVERWRITE, UPSERT_BY_KEY, UPSERT_BY_KEY_AND_TIMESTAMP, SCD2, OVERWRITE_BY_PARTITION)
}

class StrategyNameDeserializer extends JsonDeserializer[StrategyType] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): StrategyType = {
    val value = jp.readValueAs[String](classOf[String])
    StrategyType.fromString(value)
  }
}
