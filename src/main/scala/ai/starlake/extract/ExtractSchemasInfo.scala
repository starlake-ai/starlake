package ai.starlake.extract

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  JsonDeserializer,
  JsonSerializer,
  SerializerProvider
}
import org.json4s.MappingException

@JsonSerialize(using = classOf[SanitizeStrategySerializer])
@JsonDeserialize(using = classOf[SanitizeStrategyDeserializer])
sealed trait SanitizeStrategy
case object OnLoad extends SanitizeStrategy
case object OnExtract extends SanitizeStrategy

class SanitizeStrategySerializer extends JsonSerializer[SanitizeStrategy] {
  override def serialize(
    value: SanitizeStrategy,
    gen: JsonGenerator,
    serializers: SerializerProvider
  ): Unit = {
    val strValue = value match {
      case OnLoad    => "ON_LOAD"
      case OnExtract => "ON_EXTRACT"
    }
    gen.writeString(strValue)
  }
}

class SanitizeStrategyDeserializer extends JsonDeserializer[SanitizeStrategy] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): SanitizeStrategy = {
    val value = jp.readValueAs[String](classOf[String])
    value match {
      case "ON_LOAD"    => OnLoad
      case "ON_EXTRACT" => OnExtract
      case x            => throw new MappingException("Can't convert " + x + " to SanitizeStrategy")
    }
  }
}
