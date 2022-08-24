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
@JsonDeserialize(using = classOf[UserTypeDeserializer])
sealed case class UserType(value: String) {
  override def toString: String = value
}

object UserType {

  def fromString(value: String): UserType = {
    value.toUpperCase match {
      case "SA" | "SERVICEACCOUNT" => UserType.SA
      case "USER"                  => UserType.USER
      case "GROUP"                 => UserType.GROUP
    }
  }

  object SA extends UserType("SA")

  object USER extends UserType("USER")

  object GROUP extends UserType("GROUP")

  val formats: Set[UserType] = Set(SA, USER, GROUP)
}

class UserTypeDeserializer extends JsonDeserializer[UserType] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): UserType = {
    val value = jp.readValueAs[String](classOf[String])
    UserType.fromString(value)
  }
}
