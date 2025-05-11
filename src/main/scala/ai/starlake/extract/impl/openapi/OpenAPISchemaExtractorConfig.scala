package ai.starlake.extract.impl.openapi

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  JsonDeserializer,
  JsonSerializer,
  SerializerProvider
}
import org.json4s.MappingException

import java.util.regex.Pattern

@JsonSerialize(using = classOf[RouteExplosionStrategySerializer])
@JsonDeserialize(using = classOf[RouteExplosionStrategyDeserializer])
sealed trait RouteExplosionStrategy
case object All extends RouteExplosionStrategy
case object Array extends RouteExplosionStrategy
case object `Object` extends RouteExplosionStrategy

class RouteExplosionStrategySerializer extends JsonSerializer[RouteExplosionStrategy] {
  override def serialize(
    value: RouteExplosionStrategy,
    gen: JsonGenerator,
    serializers: SerializerProvider
  ): Unit = {
    val strValue = value match {
      case All    => "ALL"
      case Array  => "ARRAY"
      case Object => "OBJECT"
    }
    gen.writeString(strValue)
  }
}

class RouteExplosionStrategyDeserializer extends JsonDeserializer[RouteExplosionStrategy] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): RouteExplosionStrategy = {
    val value = jp.readValueAs[String](classOf[String])
    value match {
      case "ALL"    => All
      case "ARRAY"  => Array
      case "OBJECT" => Object
      case x => throw new MappingException("Can't convert " + x + " to RouteExplosionStrategy")
    }
  }
}

@JsonSerialize(using = classOf[HttpOperationSerializer])
@JsonDeserialize(using = classOf[HttpOperationDeserializer])
sealed trait HttpOperation
case object Get extends HttpOperation
case object Post extends HttpOperation

class HttpOperationDeserializer extends JsonDeserializer[HttpOperation] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): HttpOperation = {
    val value = jp.readValueAs[String](classOf[String])
    value match {
      case "GET"  => Get
      case "POST" => Post
      case x      => throw new MappingException("Can't convert " + x + " to HttpOperation")
    }
  }
}

final class HttpOperationSerializer extends JsonSerializer[HttpOperation] {

  override def serialize(
    value: HttpOperation,
    gen: JsonGenerator,
    serializers: SerializerProvider
  ): Unit = {
    val strValue = value match {
      case Get  => "GET"
      case Post => "POST"
    }
    gen.writeString(strValue)
  }
}

case class OpenAPIRouteExplosion(
  on: RouteExplosionStrategy = All,
  exclude: List[Pattern] = Nil,
  rename: Map[String, Pattern] = Map.empty
) {
  @JsonCreator
  private def this() = { // Should never be called. Here for Jackson deserialization only
    this(exclude = Nil)
  }
}

case class OpenAPISchema(
  include: List[Pattern] = List(Pattern.compile(".*")),
  exclude: List[Pattern] = Nil
) {
  @JsonCreator
  private def this() = { // Should never be called. Here for Jackson deserialization only
    this(exclude = Nil)
  }
}

case class OpenAPIRoute(
  paths: List[Pattern] = List(Pattern.compile(".*")),
  as: Option[String] = None,
  operations: Set[HttpOperation] = Set(Get),
  exclude: List[Pattern] = Nil,
  excludeFields: List[Pattern] = Nil,
  explode: Option[OpenAPIRouteExplosion] = None
) {
  @JsonCreator
  private def this() = { // Should never be called. Here for Jackson deserialization only
    this(as = None)
  }
}

case class OpenAPIDomain(
  name: String,
  basePath: Option[String] = None,
  schemas: Option[OpenAPISchema] = None,
  routes: List[OpenAPIRoute] = Nil
) {
  @JsonCreator
  private def this() = { // Should never be called. Here for Jackson deserialization only
    this(name = "")
  }
}

case class OpenAPIExtractSchema(
  basePath: Option[String] = None,
  formatTypeMapping: Map[String, String] = Map.empty,
  domains: List[OpenAPIDomain] = Nil
) {
  @JsonCreator
  private def this() = {
    this(basePath = None)
  }
}
