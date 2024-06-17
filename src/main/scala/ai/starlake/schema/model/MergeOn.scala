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
@JsonSerialize(using = classOf[MergeOnSerializer])
@JsonDeserialize(using = classOf[MergeOnDeserializer])
sealed case class MergeOn(value: String) {
  override def toString: String = value
}

object MergeOn {

  def fromString(value: String): MergeOn = {
    value.toUpperCase() match {
      case "TARGET"            => MergeOn.TARGET
      case "SOURCE_AND_TARGET" => MergeOn.SOURCE_AND_TARGET
    }
  }

  object TARGET extends MergeOn("TARGET")

  object SOURCE_AND_TARGET extends MergeOn("SOURCE_AND_TARGET")

  val mergeOns: Set[MergeOn] = Set(TARGET, SOURCE_AND_TARGET)
}

class MergeOnDeserializer extends JsonDeserializer[MergeOn] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): MergeOn = {
    val value = jp.readValueAs[String](classOf[String])
    MergeOn.fromString(value)
  }
}

final class MergeOnSerializer extends JsonSerializer[MergeOn] {

  override def serialize(
    value: MergeOn,
    gen: JsonGenerator,
    serializers: SerializerProvider
  ): Unit = {
    val strValue = value.toString
    gen.writeString(strValue)
  }
}
