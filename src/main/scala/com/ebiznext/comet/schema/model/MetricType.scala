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
@JsonDeserialize(using = classOf[IndexTypeDeserializer])
sealed case class MetricType(value: String) {
  override def toString: String = value
}

object MetricType {

  def fromString(value: String): MetricType = {
    value.toUpperCase() match {
      case "DISCRETE"   => MetricType.DISCRETE
      case "CONTINUOUS" => MetricType.CONTINUOUS
      case "TEXT"       => MetricType.TEXT
      case "NONE"       => MetricType.NONE
    }
  }

  object DISCRETE extends MetricType("DISCRETE")

  object CONTINUOUS extends MetricType("CONTINUOUS")

  object TEXT extends MetricType("TEXT")

  object NONE extends MetricType("NONE")

  val indexTypes: Set[MetricType] = Set(NONE, DISCRETE, CONTINUOUS, TEXT)
}

class IndexTypeDeserializer extends JsonDeserializer[MetricType] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): MetricType = {
    val value = jp.readValueAs[String](classOf[String])
    MetricType.fromString(value)
  }
}
