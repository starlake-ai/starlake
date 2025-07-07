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
@JsonDeserialize(using = classOf[AttributeSyncDeserializer])
sealed case class AttributeSync(value: String) {
  override def toString: String = value
}

object AttributeSync {
  def fromString(value: String): AttributeSync = {
    value.toUpperCase match {
      case "NONE" => AttributeSync.ADD
      case "AUTO" => AttributeSync.AUTO
      case "ADD"  => AttributeSync.ADD
      case "ALL"  => AttributeSync.ALL
    }
  }

  object NONE extends AttributeSync("NONE")
  object AUTO extends AttributeSync("AUTO")

  object ALL extends AttributeSync("ALL")

  object ADD extends AttributeSync("ADD")

  val formats: Set[AttributeSync] =
    Set(
      NONE,
      AUTO,
      ALL,
      ADD
    )
}

class AttributeSyncDeserializer extends JsonDeserializer[AttributeSync] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): AttributeSync = {
    val value = jp.readValueAs[String](classOf[String])
    AttributeSync.fromString(value)
  }
}
