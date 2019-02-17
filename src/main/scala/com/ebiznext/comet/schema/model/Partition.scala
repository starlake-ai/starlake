package com.ebiznext.comet.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}


/**
  *
  * @param strategy
  * @param absolute
  * @param attributes
  */
@JsonDeserialize(using = classOf[PartitionDeserializer])
case class Partition(strategy: Option[Double], absolute: Option[Boolean], attributes: Option[List[String]]) {
  def getAtrributes(): List[String] = attributes.getOrElse(Nil)

  def getStrategy() = strategy.getOrElse(0.0)

  def isAbsolute() = absolute.getOrElse(false)
}


class PartitionDeserializer extends JsonDeserializer[Partition] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Partition = {
    val node: JsonNode = jp.getCodec().readTree[JsonNode](jp)
    deserialize(node)
  }

  def deserialize(node: JsonNode): Partition = {
    def isNull(field: String): Boolean =
      node.get(field) == null || node.get(field).isNull

    val strategy =
      if (isNull("strategy")) 0.0
      else
        node.get("strategy").asDouble()

    val absolute =
      if (isNull("absolute")) false
      else
        node.get("absolute").asBoolean()


    import scala.collection.JavaConverters._
    val attributes =
      if (isNull("attributes")) None
      else
        Some(
          node
            .get("attributes")
            .asInstanceOf[ArrayNode]
            .elements
            .asScala
            .toList
            .map(_.asText())
        )
    Partition(Some(strategy), Some(absolute), attributes)
  }
}
