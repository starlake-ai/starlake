package com.ebiznext.comet.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}

/**
  *
  * @param sampling : 0.0 means no sampling, > 0  && < 1 means sample datsets, >=1 absolue number of partitions.
  * @param attributes : Attributes used to partition de data.
  */
@JsonDeserialize(using = classOf[PartitionDeserializer])
case class Partition(
  sampling: Option[Double],
  attributes: Option[List[String]]
) {
  def getAtrributes(): List[String] = attributes.getOrElse(Nil)

  def getSampling() = sampling.getOrElse(0.0)

}

class PartitionDeserializer extends JsonDeserializer[Partition] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Partition = {
    val node: JsonNode = jp.getCodec().readTree[JsonNode](jp)
    deserialize(node)
  }

  def deserialize(node: JsonNode): Partition = {
    def isNull(field: String): Boolean =
      node.get(field) == null || node.get(field).isNull

    val sampling =
      if (isNull("sampling")) 0.0
      else
        node.get("sampling").asDouble()

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
    Partition(Some(sampling), attributes)
  }
}
