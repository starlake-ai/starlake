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

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

/**
  * Recognized file type format. This will select  the correct parser
  *
  * @param value : SIMPLE_JSON, JSON of DSV
  *              Simple Json is made of a single level attributes of simple types (no arrray or map or sub objects)
  */

@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[SinkTypeDeserializer])
sealed case class SinkType(value: String) {
  override def toString: String = value
}

object SinkType {

  def fromString(value: String): SinkType = {
    value.toUpperCase match {
      case "NONE" => SinkType.None
      case "FS"   => SinkType.FS
      case "JDBC" => SinkType.JDBC
      case "BQ"   => SinkType.BQ
      case "ES"   => SinkType.ES
    }
  }

  object None extends SinkType("None")

  object FS extends SinkType("FS")

  object BQ extends SinkType("BQ")

  object ES extends SinkType("ES")

  object JDBC extends SinkType("JDBC")

  val sinks: Set[SinkType] = Set(None, FS, BQ, ES, JDBC)
}

class SinkTypeDeserializer extends JsonDeserializer[SinkType] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): SinkType = {
    val value = jp.readValueAs[String](classOf[String])
    SinkType.fromString(value)
  }
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[BigQuerySink], name = "BQ"),
    new JsonSubTypes.Type(value = classOf[EsSink], name = "ES"),
    new JsonSubTypes.Type(value = classOf[JdbcSink], name = "JDBC")
  )
)
sealed abstract class Sink(val `type`: SinkType)

@JsonTypeName("BQ")
final case class BigQuerySink(
  location: Option[String] = None,
  timestamp: Option[String] = None,
  clustering: Option[Seq[String]] = None,
  days: Option[Int] = None
) extends Sink(SinkType.BQ)

@JsonTypeName("ES")
final case class EsSink(id: Option[String] = None, timestamp: Option[String] = None)
    extends Sink(SinkType.ES)

@JsonTypeName("JDBC")
final case class JdbcSink(
  connection: String,
  partitions: Option[Int] = None,
  batchsize: Option[Int] = None
) extends Sink(SinkType.JDBC)

object Sink {

  def fromString(sinkTypeStr: String): Sink = {
    val sinkType = SinkType.fromString(sinkTypeStr)
    sinkType match {
      case SinkType.BQ => BigQuerySink()
      case SinkType.ES => EsSink()
      case _           => throw new Exception(s"Unsupported creation of SinkType from $sinkType")
    }
  }
}
