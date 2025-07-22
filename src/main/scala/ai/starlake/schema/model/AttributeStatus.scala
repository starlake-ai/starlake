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
  *   The value of the attribute sync (NONE, AUTO, ADD, ALL)
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[AttributeStatusDeserializer])
sealed case class AttributeStatus(value: String) {
  override def toString: String = value
}

object AttributeStatus {
  def fromString(value: String): AttributeStatus = {
    value.toUpperCase match {
      case "ADD"       => AttributeStatus.ADDED
      case "MODIFIED"  => AttributeStatus.MODIFIED
      case "REMOVED"   => AttributeStatus.REMOVED
      case "UNCHANGED" => AttributeStatus.UNCHANGED
      case _           => throw new IllegalArgumentException(s"Unknown attribute status: $value")
    }
  }
  object ADDED extends AttributeStatus("ADD")

  object MODIFIED extends AttributeStatus("MODIFIED")

  object REMOVED extends AttributeStatus("REMOVED")

  object UNCHANGED extends AttributeStatus("UNCHANGED")

  val formats: Set[AttributeStatus] =
    Set(
      ADDED,
      MODIFIED,
      REMOVED,
      UNCHANGED
    )
}

class AttributeStatusDeserializer extends JsonDeserializer[AttributeStatus] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): AttributeStatus = {
    val value = jp.readValueAs[String](classOf[String])
    AttributeStatus.fromString(value)
  }
}
