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

@JsonSerialize(using = classOf[EngineSerializer])
@JsonDeserialize(using = classOf[EngineDeserializer])
sealed abstract class Engine(value: String) {
  override def toString: String = value
}

object Engine {

  def fromString(value: String): Engine = {
    value.toUpperCase() match {
      case "BQ" | "BIGQUERY"      => Engine.BQ
      case "JDBC"                 => Engine.JDBC
      case "SPARK" | "DATABRICKS" => Engine.SPARK
      case custom                 => Engine.Custom(custom)
    }
  }

  final case class Custom(value: String) extends Engine(value)

  object BQ extends Engine("BQ")

  object SPARK extends Engine("SPARK")

  object JDBC extends Engine("JDBC")
}

class EngineDeserializer extends JsonDeserializer[Engine] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Engine = {
    val value = jp.readValueAs[String](classOf[String])
    Engine.fromString(value)
  }
}

final class EngineSerializer extends JsonSerializer[Engine] {

  override def serialize(
    value: Engine,
    gen: JsonGenerator,
    serializers: SerializerProvider
  ): Unit = {
    val strValue = value.toString
    gen.writeString(strValue)
  }
}
