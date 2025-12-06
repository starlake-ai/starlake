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
@JsonSerialize(`using` = classOf[ToStringSerializer])
@JsonDeserialize(`using` = classOf[AttributeSyncDeserializer])
sealed case class TableSync(value: String) {
  override def toString: String = value
}

object TableSync {
  def fromString(value: String): TableSync = {
    value.toUpperCase match {
      case "NONE" => TableSync.NONE // do not sync attributes
      case "ADD"  => TableSync.ADD // add attributes to the schema if they are not present
      case "ALL"  => TableSync.ALL // add or remove attributes to match the source schema
    }
  }

  object NONE extends TableSync("NONE")

  object ALL extends TableSync("ALL")

  object ADD extends TableSync("ADD")

  val strategies: Set[TableSync] =
    Set(
      NONE,
      ALL,
      ADD
    )
}

class AttributeSyncDeserializer extends JsonDeserializer[TableSync] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): TableSync = {
    val value = jp.readValueAs[String](classOf[String])
    TableSync.fromString(value)
  }
}
