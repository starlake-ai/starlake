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

import ai.starlake.config.Settings
import ai.starlake.config.Settings.Connection
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonTypeName}
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
@JsonDeserialize(using = classOf[ConnectionTypeDeserializer])
sealed case class ConnectionType(value: String) {
  override def toString: String = value
}

object ConnectionType {

  def fromString(value: String): ConnectionType = {
    value.toUpperCase match {
      case "GCPLOG"                                                        => ConnectionType.GCPLOG
      case "LOCAL" | "FS" | "FILESYSTEM" | "HIVE" | "DATABRICKS" | "SPARK" => ConnectionType.FS
      case "JDBC"                                                          => ConnectionType.JDBC
      case "BIGQUERY" | "BQ"                                               => ConnectionType.BQ
      case "ES" | "ELASTICSEARCH"                                          => ConnectionType.ES
      case "KAFKA"                                                         => ConnectionType.KAFKA
      case _ => throw new Exception(s"Unsupported ConnectionType $value")
    }
  }

  object FS extends ConnectionType("FS")
  object BQ extends ConnectionType("BQ")
  object ES extends ConnectionType("ES")
  object KAFKA extends ConnectionType("KAFKA")
  object JDBC extends ConnectionType("JDBC")

  object GCPLOG extends ConnectionType("GCPLOG")

  val sinks: Set[ConnectionType] = Set(FS, BQ, ES, KAFKA, JDBC, GCPLOG)
}

class ConnectionTypeDeserializer extends JsonDeserializer[ConnectionType] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): ConnectionType = {
    val value = jp.readValueAs[String](classOf[String])
    ConnectionType.fromString(value)
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

sealed abstract class Sink {
  val connectionRef: Option[String]
  def toAllSinks(): AllSinks

  def getConnectionType()(implicit
    settings: Settings
  ): ConnectionType = {
    getConnection().getType()
  }

  def getConnection()(implicit
    settings: Settings
  ): Connection = {
    val ref = connectionRef.getOrElse(settings.appConfig.connectionRef)
    settings.appConfig.connections(ref)
  }

  @JsonIgnore
  def connectionRefOptions(defaultConnectionName: String)(implicit
    settings: Settings
  ): Map[String, String] = {
    val ref = connectionRef.getOrElse(defaultConnectionName)
    settings.appConfig.connections
      .get(ref)
      .map(_.options)
      .getOrElse(Map.empty)
  }
}

object Sink {
  def xlsfromConnectionType(sinkTypeStr: String): Sink = {
    val sinkType = ConnectionType.fromString(sinkTypeStr)
    sinkType match {
      case ConnectionType.FS => FsSink(connectionRef = Some(sinkTypeStr))
      case ConnectionType.BQ => BigQuerySink(connectionRef = Some(sinkTypeStr))
      case ConnectionType.ES => EsSink(connectionRef = Some(sinkTypeStr))
      case _ => throw new Exception(s"Unsupported creation of SinkType from $sinkType")
    }
  }
}

case class AllSinks(
  // All sinks
  connectionRef: Option[String] = None,
  // depending on the connection.type coming from the connection ref, some of the options below may be required
  // BigQuery
  timestamp: Option[String] = None,
  clustering: Option[Seq[String]] = None,
  days: Option[Int] = None,
  requirePartitionFilter: Option[Boolean] = None,
  materializedView: Option[Boolean] = None,
  enableRefresh: Option[Boolean] = None,
  refreshIntervalMs: Option[Long] = None,
  // ES
  id: Option[String] = None,
  // timestamp: Option[String] = None,
  // options: Option[Map[String, String]] = None,

  // FS
  format: Option[String] = None,
  extension: Option[String] = None,

  // clustering: Option[Seq[String]] = None,
  partition: Option[Partition] = None,
  dynamicPartitionOverwrite: Option[Boolean] = None,
  coalesce: Option[Boolean] = None,
  options: Option[Map[String, String]] = None
  // JDBC
) {
  def checkValidity()(settings: Settings): List[ValidationMessage] = {
    var errors = List.empty[ValidationMessage]
    if (connectionRef.nonEmpty && !settings.appConfig.connections.contains(connectionRef.get)) {
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "connectionRef",
        "No connectionRef provided for sink. Please provide a connectionRef in the sink or in the application.conf file"
      )
    }
    errors
  }

  def merge(child: AllSinks): AllSinks = {
    AllSinks(
      child.connectionRef.orElse(this.connectionRef),
      child.timestamp.orElse(this.timestamp),
      child.clustering.orElse(this.clustering),
      child.days.orElse(this.days),
      child.requirePartitionFilter.orElse(this.requirePartitionFilter),
      child.materializedView.orElse(this.materializedView),
      child.enableRefresh.orElse(this.enableRefresh),
      child.refreshIntervalMs.orElse(this.refreshIntervalMs),
      child.id.orElse(this.id),
      child.format.orElse(this.format),
      child.extension.orElse(this.extension),
      child.partition.orElse(this.partition),
      child.dynamicPartitionOverwrite.orElse(this.dynamicPartitionOverwrite),
      child.coalesce.orElse(this.coalesce),
      child.options.orElse(this.options)
    )
  }
  def toAllSinks(): AllSinks = this
  def getSink()(implicit settings: Settings): Sink = {
    val ref = this.connectionRef.getOrElse(settings.appConfig.connectionRef)

    val connection = settings.appConfig.connections
      .getOrElse(ref, throw new Exception(s"Could not find connection $ref"))
    val options = this.options.getOrElse(connection.options)
    val allSinksWithOptions = this.copy(options = Some(options))
    connection.getType() match {
      case ConnectionType.GCPLOG => GcpLogSink.fromAllSinks(allSinksWithOptions)
      case ConnectionType.FS     => FsSink.fromAllSinks(allSinksWithOptions)
      case ConnectionType.JDBC   => JdbcSink.fromAllSinks(allSinksWithOptions)
      case ConnectionType.BQ     => BigQuerySink.fromAllSinks(allSinksWithOptions)
      case ConnectionType.ES     => EsSink.fromAllSinks(allSinksWithOptions)
      case ConnectionType.KAFKA  => KafkaSink.fromAllSinks(allSinksWithOptions)
      case _ => throw new Exception(s"Unsupported SinkType sink type ${connection.getType()}")

    }
  }
}

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
  connectionRef: Option[String] = None,
  timestamp: Option[String] = None,
  dynamicPartitionOverwrite: Option[Boolean] = None,
  clustering: Option[Seq[String]] = None,
  days: Option[Int] = None,
  requirePartitionFilter: Option[Boolean] = None,
  materializedView: Option[Boolean] = None,
  enableRefresh: Option[Boolean] = None,
  refreshIntervalMs: Option[Long] = None
) extends Sink {
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef,
      timestamp,
      clustering,
      days,
      requirePartitionFilter,
      materializedView,
      enableRefresh,
      refreshIntervalMs
    )
  }
}

object BigQuerySink {
  def fromAllSinks(allSinks: AllSinks): BigQuerySink = {
    BigQuerySink(
      connectionRef = allSinks.connectionRef,
      timestamp = allSinks.timestamp,
      dynamicPartitionOverwrite = allSinks.dynamicPartitionOverwrite,
      clustering = allSinks.clustering,
      days = allSinks.days,
      requirePartitionFilter = allSinks.requirePartitionFilter,
      materializedView = allSinks.materializedView,
      enableRefresh = allSinks.enableRefresh,
      refreshIntervalMs = allSinks.refreshIntervalMs
    )
  }
}

/** When the sink *type* field is set to ES, the options below should be provided. Elasticsearch
  * options are specified in the application.conf file.
  * @param id:
  *   Attribute to use as id of the document. Generated by Elasticseach if not specified.
  * @param timestamp:
  *   Timestamp field format as expected by Elasticsearch ("{beginTs|yyyy.MM.dd}" for example).
  */
@JsonTypeName("ES")
case class EsSink(
  connectionRef: Option[String] = None,
  id: Option[String] = None,
  timestamp: Option[String] = None,
  options: Option[Map[String, String]] = None
) extends Sink {
  def getOptions(): Map[String, String] = options.getOrElse(Map.empty)
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef,
      timestamp = timestamp,
      id = id,
      options = options
    )
  }
}

object EsSink {
  def fromAllSinks(allSinks: AllSinks): EsSink = {
    EsSink(
      allSinks.connectionRef,
      allSinks.id,
      allSinks.timestamp,
      allSinks.options
    )
  }
}
// We had to set format and extension outside options because of the bug below
// https://www.google.fr/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwjo9qr3v4PxAhWNohQKHfh1CqoQFjAAegQIAhAD&url=https%3A%2F%2Fgithub.com%2FFasterXML%2Fjackson-module-scala%2Fissues%2F218&usg=AOvVaw02niMBgrqd-BWw7-e1YQfc
@JsonTypeName("FS")
case class FsSink(
  connectionRef: Option[String] = None,
  format: Option[String] = None,
  extension: Option[String] = None,
  clustering: Option[Seq[String]] = None,
  partition: Option[Partition] = None,
  dynamicPartitionOverwrite: Option[Boolean] = None,
  coalesce: Option[Boolean] = None,
  options: Option[Map[String, String]] = None
) extends Sink {

  def getFormat()(implicit settings: Settings) = {
    format.getOrElse(settings.appConfig.defaultWriteFormat)
  }

  def getPartitionByClauseSQL(): String =
    partition.map(_.attributes).map(_.mkString("PARTITIONED BY (", ",", ")")) getOrElse ""

  def getClusterByClauseSQL(): String =
    clustering.map(_.mkString("CLUSTERED BY (", ",", ")")) getOrElse ""

  def getTableOptionsClause(): String = {
    val opts = options.getOrElse(Map.empty)
    if (opts.isEmpty) {
      ""
    } else {
      opts.map { case (k, v) => s"'$k'='$v'" }.mkString("OPTIONS(", ",", ")")
    }
  }

  def getOptions(): Map[String, String] = options.getOrElse(Map.empty)

  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef,
      format = format,
      extension = extension,
      clustering = clustering,
      partition = partition,
      coalesce = coalesce,
      options = options
    )
  }
}

@JsonTypeName("GCPLOG")
case class GcpLogSink(
  connectionRef: Option[String] = None,
  options: Option[Map[String, String]] = None
) extends Sink {
  def getOptions(): Map[String, String] = options.getOrElse(Map.empty)
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef,
      options = options
    )
  }
}

@JsonTypeName("KAFKA")
case class KafkaSink(
  connectionRef: Option[String] = None,
  options: Option[Map[String, String]] = None
) extends Sink {
  def getOptions(): Map[String, String] = options.getOrElse(Map.empty)
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef,
      options = options
    )
  }
}

object KafkaSink {
  def fromAllSinks(allSinks: AllSinks): KafkaSink = {
    KafkaSink(
      allSinks.connectionRef,
      allSinks.options
    )
  }
}

object GcpLogSink {
  def fromAllSinks(allSinks: AllSinks): GcpLogSink = {
    GcpLogSink(
      allSinks.connectionRef,
      allSinks.options
    )
  }
}

object FsSink {
  def fromAllSinks(allSinks: AllSinks): FsSink = {
    FsSink(
      allSinks.connectionRef,
      allSinks.format,
      allSinks.extension,
      allSinks.clustering,
      allSinks.partition,
      allSinks.dynamicPartitionOverwrite,
      allSinks.coalesce,
      allSinks.options
    )
  }
}

/** When the sink *type* field is set to JDBC, the options below should be provided.
  * @param connectionRef:
  *   Connection String
  * @param partitions:
  *   Number of Spark partitions
  * @param batchsize:
  *   Batch size of each JDBC bulk insert
  */
@JsonTypeName("JDBC")
case class JdbcSink(connectionRef: Option[String] = None) extends Sink {
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef
    )
  }
}

object JdbcSink {
  def fromAllSinks(allSinks: AllSinks): JdbcSink = {
    JdbcSink(
      allSinks.connectionRef
    )
  }
}
