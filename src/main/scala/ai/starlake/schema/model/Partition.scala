/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import scala.collection.JavaConverters._

/** @param sampling
  *   : 0.0 means no sampling, > 0 && < 1 means sample dataset, >=1 absolute number of partitions.
  * @param attributes
  *   : Attributes used to partition de dataset.
  */
@JsonDeserialize(using = classOf[PartitionDeserializer])
case class Partition(
  attributes: List[String] = Nil,
  attribute: Option[String] = None,
  options: List[String] = Nil
) {
  def getAttributes(): List[String] = if (attributes.isEmpty) attribute.toList else attributes
  def getOptions(): List[String] = options

}

class PartitionDeserializer extends JsonDeserializer[Partition] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Partition = {
    val node: JsonNode = jp.getCodec().readTree[JsonNode](jp)
    deserialize(node)
  }

  def deserialize(node: JsonNode): Partition = {
    def isNull(field: String): Boolean =
      node.get(field) == null || node.get(field).isNull

    def deserializeListOfString(field: String) = {
      if (isNull(field)) Nil
      else
        node
          .get(field)
          .asInstanceOf[ArrayNode]
          .elements
          .asScala
          .toList
          .map(_.asText())
    }

    val attributes = deserializeListOfString("attributes")
    val attribute = if (isNull("attribute")) None else Some(node.get("sampling").asText())
    val options = deserializeListOfString("options")

    Partition(attributes, attribute, options)
  }

}
