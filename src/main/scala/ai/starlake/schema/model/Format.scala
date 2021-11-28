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
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

/** Recognized file type format. This will select the correct parser
  *
  * @param value
  *   : SIMPLE_JSON, JSON of DSV Simple Json is made of a single level attributes of simple types
  *   (no arrray or map or sub objects)
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[FormatDeserializer])
sealed case class Format(value: String) {
  override def toString: String = value
}

object Format {

  def fromString(value: String): Format = {
    value.toUpperCase match {
      case "DSV"                 => Format.DSV
      case "POSITION"            => Format.POSITION
      case "JSON" | "ARRAY_JSON" => Format.JSON
      case "SIMPLE_JSON"         => Format.SIMPLE_JSON
      case "XML"                 => Format.XML
      case "TEXT_XML"            => Format.TEXT_XML
      case "KAFKA"               => Format.KAFKA
      case "KAFKASTREAM"         => Format.KAFKASTREAM
    }
  }

  object DSV extends Format("DSV")

  object POSITION extends Format("POSITION")

  object KAFKA extends Format("KAFKA")

  object KAFKASTREAM extends Format("KAFKASTREAM")

  object SIMPLE_JSON extends Format("SIMPLE_JSON")

  object JSON extends Format("JSON")

  object XML extends Format("XML")

  object TEXT_XML extends Format("TEXT_XML")

  val formats: Set[Format] =
    Set(DSV, POSITION, SIMPLE_JSON, JSON, XML, TEXT_XML, KAFKA, KAFKASTREAM)
}

class FormatDeserializer extends JsonDeserializer[Format] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Format = {
    val value = jp.readValueAs[String](classOf[String])
    Format.fromString(value)
  }
}
