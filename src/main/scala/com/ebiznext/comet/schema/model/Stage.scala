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
  * Big versus Fast data ingestion. Are we ingesting a file or a message stream ?
  *
  * @param value : FILE or STREAM
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[StageDeserializer])
sealed case class Stage(value: String) {
  override def toString: String = value
}

object Stage {

  def fromString(value: String): Stage = {
    value.toUpperCase() match {
      case "UNIT"   => Stage.UNIT
      case "GLOBAL" => Stage.GLOBAL
    }
  }

  object UNIT extends Stage("UNIT")

  object GLOBAL extends Stage("GLOBAL")

  val stages: Set[Stage] = Set(UNIT, GLOBAL)
}

class StageDeserializer extends JsonDeserializer[Stage] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Stage = {
    val value = jp.readValueAs[String](classOf[String])
    Stage.fromString(value)
  }
}
