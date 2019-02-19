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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

/**
  * This attribute property let us know what statistics should be computed for this field
  * when analyze is active.
  * @param value : DISCRETE or CONTINUOUS or TEXT or NONE
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[StatDeserializer])
sealed case class Stat(value: String) {
  override def toString: String = value
}

object Stat {

  def fromString(value: String): Stat = {
    value.toUpperCase() match {
      case "DISCRETE"   => Stat.DISCRETE
      case "CONTINUOUS" => Stat.CONTINUOUS
      case "TEXT"       => Stat.TEXT
      case "NONE"       => Stat.NONE
    }
  }

  object DISCRETE extends Stat("DISCRETE")

  object CONTINUOUS extends Stat("CONTINUOUS")

  object TEXT extends Stat("TEXT")

  object NONE extends Stat("NONE")

  val stats: Set[Stat] = Set(NONE, DISCRETE, CONTINUOUS, TEXT)
}

class StatDeserializer extends JsonDeserializer[Stat] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Stat = {
    val value = jp.readValueAs[String](classOf[String])
    Stat.fromString(value)
  }
}
