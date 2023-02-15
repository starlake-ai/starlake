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

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

/** Recognized file type format. This will select the correct parser
  *
  * @param value
  *   : NONE, FS, JDBC, BQ, ES One of the possible supported sinks
  */

@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[SinkTypeDeserializer])
sealed case class SinkType(value: String) {
  override def toString: String = value
}

object SinkType {

  def fromString(value: String): SinkType = {
    value.toUpperCase match {
      case "NONE" | "NONESINK"                => SinkType.None
      case "FS" | "FSSINK"                    => SinkType.FS
      case "GENERIC" | "JDBC" | "JDBCSINK"    => SinkType.JDBC
      case "BQ" | "BIGQUERY" | "BIGQUERYSINK" => SinkType.BQ
      case "ES" | "ESSINK"                    => SinkType.ES
      case "KAFKA" | "KAFKASINK"              => SinkType.KAFKA
    }
  }

  object None extends SinkType("None")

  object FS extends SinkType("FS")

  object BQ extends SinkType("BQ")

  object ES extends SinkType("ES")

  object KAFKA extends SinkType("KAFKA")

  object JDBC extends SinkType("JDBC")

  val sinks: Set[SinkType] = Set(None, FS, BQ, ES, KAFKA, JDBC)
}

class SinkTypeDeserializer extends JsonDeserializer[SinkType] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): SinkType = {
    val value = jp.readValueAs[String](classOf[String])
    SinkType.fromString(value)
  }
}

/** Once ingested, files may be sinked to BigQuery, Elasticsearch or any JDBC compliant Database.
  * @param `type`:
  *   Enum
  *   - JDBC : dataset will be sinked to a JDBC Database. See JdbcSink below
  *   - ES : dataset is indexed into Elasticsearch. See EsSink below
  *   - BQ : Dataset is sinked to BigQuery. See BigQuerySink below
  *   - None: Don't sink. This is the default.
  */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.EXISTING_PROPERTY,
  property = "type"
)
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[FsSink], name = "FS"),
    new JsonSubTypes.Type(value = classOf[NoneSink], name = "None"),
    new JsonSubTypes.Type(value = classOf[BigQuerySink], name = "BQ"),
    new JsonSubTypes.Type(value = classOf[EsSink], name = "ES"),
    new JsonSubTypes.Type(value = classOf[JdbcSink], name = "JDBC")
  )
)
sealed abstract class Sink(val `type`: String) {
  def getType(): SinkType = SinkType.fromString(`type`)
  def name: Option[String]
  def options: Option[Map[String, String]]
  def getOptions: Map[String, String] = options.getOrElse(Map.empty)
}

/*trait SinkTrait extends Sink{
  @JsonIgnore
  def getOptions:Map[String, String] = if(options==null) Map.empty else options
}*/
/** When the sink *type* field is set to BQ, the options below should be provided.
  * @param location
  *   : Database location (EU, US, ...)
  * @param timestamp:
  *   The timestamp column to use for table partitioning if any. No partitioning by default
  * @param clustering:
  *   List of ordered columns to use for table clustering
  * @param days:
  *   Number of days before this table is set as expired and deleted. Never by default.
  * @param requirePartitionFilter:
  *   Should be require a partition filter on every request ? No by default.
  */
@JsonTypeName("BQ")
final case class BigQuerySink(
  override val name: Option[String] = None,
  location: Option[String] = None,
  timestamp: Option[String] = None,
  clustering: Option[Seq[String]] = None,
  days: Option[Int] = None,
  requirePartitionFilter: Option[Boolean] = None,
  options: Option[Map[String, String]] = None,
  materializedView: Option[Boolean] = None
) extends Sink(SinkType.BQ.value)

/** When the sink *type* field is set to ES, the options below should be provided. Elasticsearch
  * options are specified in the application.conf file.
  * @param id:
  *   Attribute to use as id of the document. Generated by Elasticseach if not specified.
  * @param timestamp:
  *   Timestamp field format as expected by Elasticsearch ("{beginTs|yyyy.MM.dd}" for example).
  */
@JsonTypeName("ES")
final case class EsSink(
  override val name: Option[String] = None,
  id: Option[String] = None,
  timestamp: Option[String] = None,
  options: Option[Map[String, String]] = None
) extends Sink(SinkType.ES.value)

@JsonTypeName("None")
final case class NoneSink(
  override val name: Option[String] = None,
  options: Option[Map[String, String]] = None
) extends Sink(SinkType.None.value)

// We had to set format and extension outside options because of the bug below
// https://www.google.fr/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwjo9qr3v4PxAhWNohQKHfh1CqoQFjAAegQIAhAD&url=https%3A%2F%2Fgithub.com%2FFasterXML%2Fjackson-module-scala%2Fissues%2F218&usg=AOvVaw02niMBgrqd-BWw7-e1YQfc
@JsonTypeName("FS")
final case class FsSink(
  override val name: Option[String] = None,
  override val options: Option[Map[String, String]] = None,
  format: Option[String] = None,
  extension: Option[String] = None,
  clustering: Option[Seq[String]] = None,
  partition: Option[Partition] = None
) extends Sink(SinkType.FS.value)

/** When the sink *type* field is set to JDBC, the options below should be provided.
  * @param connection:
  *   Connection String
  * @param partitions:
  *   Number of Spark partitions
  * @param batchsize:
  *   Batch size of each JDBC bulk insert
  */
@JsonTypeName("JDBC")
final case class JdbcSink(
  override val name: Option[String] = None,
  connection: String,
  options: Option[Map[String, String]] = None
) extends Sink(SinkType.JDBC.value)

object Sink {

  def fromType(sinkTypeStr: String): Sink = {
    val sinkType = SinkType.fromString(sinkTypeStr)
    sinkType match {
      case SinkType.None => NoneSink()
      case SinkType.FS   => FsSink()
      case SinkType.BQ   => BigQuerySink()
      case SinkType.ES   => EsSink()
      case _             => throw new Exception(s"Unsupported creation of SinkType from $sinkType")
    }
  }
}
