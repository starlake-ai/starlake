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
      case StrategyType.APPEND                     => WriteMode.APPEND
      case StrategyType.OVERWRITE                  => WriteMode.OVERWRITE
      case StrategyType.MERGE_BY_KEY               => WriteMode.APPEND
      case StrategyType.MERGE_BY_KEY_AND_TIMESTAMP => WriteMode.APPEND
      case StrategyType.SCD2                       => WriteMode.APPEND
      case _ =>
        throw new Exception("Should never happen")
    }

  }
}

object StrategyType {

  def fromString(value: String): StrategyType = {
    value.toUpperCase match {
      case "APPEND" | "MERGE"           => StrategyType.APPEND
      case "OVERWRITE"                  => StrategyType.OVERWRITE
      case "MERGE_BY_KEY"               => StrategyType.MERGE_BY_KEY
      case "MERGE_BY_KEY_AND_TIMESTAMP" => StrategyType.MERGE_BY_KEY_AND_TIMESTAMP
      case "SCD2"                       => StrategyType.SCD2

    }
  }

  object APPEND extends StrategyType("APPEND")

  object OVERWRITE extends StrategyType("OVERWRITE")

  object MERGE_BY_KEY extends StrategyType("MERGE_BY_KEY")

  object MERGE_BY_KEY_AND_TIMESTAMP extends StrategyType("MERGE_BY_KEY_AND_TIMESTAMP")

  object SCD2 extends StrategyType("SCD2")

  val strategies: Set[StrategyType] =
    Set(APPEND, OVERWRITE, MERGE_BY_KEY, MERGE_BY_KEY_AND_TIMESTAMP, SCD2)
}

class StrategyNameDeserializer extends JsonDeserializer[StrategyType] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): StrategyType = {
    val value = jp.readValueAs[String](classOf[String])
    StrategyType.fromString(value)
  }
}
