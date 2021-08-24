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

import com.ebiznext.comet.schema.model.WriteMode.{APPEND, ERROR_IF_EXISTS, IGNORE, OVERWRITE}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.spark.sql.SaveMode

/** During ingestion, should the data be appended to the previous ones or should it replace the
  * existing ones ? see Spark SaveMode for more options.
  *
  * @param value
  *   : OVERWRITE / APPEND / ERROR_IF_EXISTS / IGNORE.
  */
@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[WriteDeserializer])
sealed case class WriteMode(value: String) {
  override def toString: String = value

  def toSaveMode: SaveMode = {
    this match {
      case OVERWRITE       => SaveMode.Overwrite
      case APPEND          => SaveMode.Append
      case ERROR_IF_EXISTS => SaveMode.ErrorIfExists
      case IGNORE          => SaveMode.Ignore
      case _ =>
        throw new Exception("Should never happen")
    }
  }
}

object WriteMode {

  def fromString(value: String): WriteMode = {
    value.toUpperCase() match {
      case "OVERWRITE"       => WriteMode.OVERWRITE
      case "APPEND"          => WriteMode.APPEND
      case "ERROR_IF_EXISTS" => WriteMode.ERROR_IF_EXISTS
      case "IGNORE"          => WriteMode.IGNORE
      case _ =>
        throw new Exception(s"Invalid Write Mode try one of ${writes}")
    }
  }

  object OVERWRITE extends WriteMode("OVERWRITE")

  object APPEND extends WriteMode("APPEND")

  object ERROR_IF_EXISTS extends WriteMode("ERROR_IF_EXISTS")

  object IGNORE extends WriteMode("IGNORE")

  val writes: Set[WriteMode] = Set(OVERWRITE, APPEND, ERROR_IF_EXISTS, IGNORE)
}

class WriteDeserializer extends JsonDeserializer[WriteMode] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): WriteMode = {
    val value = jp.readValueAs[String](classOf[String])
    WriteMode.fromString(value)
  }
}
