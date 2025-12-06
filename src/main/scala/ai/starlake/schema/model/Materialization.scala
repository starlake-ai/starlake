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

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  JsonDeserializer,
  JsonSerializer,
  SerializerProvider
}

/** Big versus Fast data ingestion. Are we ingesting a file or a message stream ?
  * @param value
  *   : FILE or STREAM
  */
@JsonSerialize(`using` = classOf[MatSerializer])
@JsonDeserialize(`using` = classOf[MatDeserializer])
sealed case class Materialization(value: String) {
  override def toString: String = value
}

object Materialization {

  def fromString(value: String): Materialization = {
    value.toUpperCase() match {
      case "HYBRID"            => Materialization.HYBRID
      case "TABLE"             => Materialization.TABLE
      case "VIEW"              => Materialization.VIEW
      case "MATERIALIZED_VIEW" => Materialization.MATERIALIZED_VIEW
    }
  }

  object HYBRID extends Materialization("HYBRID")

  object TABLE extends Materialization("TABLE")

  object VIEW extends Materialization("VIEW")

  object MATERIALIZED_VIEW extends Materialization("MATERIALIZED_VIEW")

  val mats: Set[Materialization] = Set(TABLE, VIEW, MATERIALIZED_VIEW, HYBRID)
}

class MatDeserializer extends JsonDeserializer[Materialization] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Materialization = {
    val value = jp.readValueAs[String](classOf[String])
    Materialization.fromString(value)
  }
}

final class MatSerializer extends JsonSerializer[Materialization] {

  override def serialize(
    value: Materialization,
    gen: JsonGenerator,
    serializers: SerializerProvider
  ): Unit = {
    val strValue = value.toString
    gen.writeString(strValue)
  }
}
