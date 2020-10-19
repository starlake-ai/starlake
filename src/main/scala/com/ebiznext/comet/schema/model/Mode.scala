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

package com.ebiznext.comet.schema.model

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  JsonDeserializer,
  JsonSerializer,
  SerializerProvider
}

/** Big versus Fast data ingestion. Are we ingesting a file or a message stream ?
  * @param value : FILE or STREAM
  */
@JsonSerialize(using = classOf[ModeSerializer])
@JsonDeserialize(using = classOf[ModeDeserializer])
sealed case class Mode(value: String) {
  override def toString: String = value
}

object Mode {

  def fromString(value: String): Mode = {
    value.toUpperCase() match {
      case "FILE"            => Mode.FILE
      case "STREAM"          => Mode.STREAM
      case "FILE_AND_STREAM" => Mode.FILE_AND_STREAM
    }
  }

  object FILE extends Mode("FILE")

  object STREAM extends Mode("STREAM")

  object FILE_AND_STREAM extends Mode("FILE_AND_STREAM")

  val modes: Set[Mode] = Set(FILE, STREAM, FILE_AND_STREAM)
}

class ModeDeserializer extends JsonDeserializer[Mode] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Mode = {
    val value = jp.readValueAs[String](classOf[String])
    Mode.fromString(value)
  }
}

final class ModeSerializer extends JsonSerializer[Mode] {

  override def serialize(
    value: Mode,
    gen: JsonGenerator,
    serializers: SerializerProvider
  ): Unit = {
    val strValue = value.toString
    gen.writeString(strValue)
  }
}
