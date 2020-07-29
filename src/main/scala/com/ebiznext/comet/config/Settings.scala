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

package com.ebiznext.comet.config

import java.io.ObjectStreamException
import java.lang.management.{ManagementFactory, RuntimeMXBean}
import java.util.concurrent.TimeUnit
import java.util.{Locale, UUID, Map => juMap}

import com.ebiznext.comet.schema.handlers.{
  AirflowLauncher,
  HdfsStorageHandler,
  LaunchHandler,
  SimpleLauncher
}
import com.ebiznext.comet.schema.model.SinkType
import com.ebiznext.comet.utils.{CometJacksonModule, CometObjectMapper, Version}
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonTypeInfo}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import configs.Configs
import configs.syntax._
import org.apache.spark.storage.StorageLevel
import org.slf4j.MDC

import scala.concurrent.duration.FiniteDuration

object Settings extends StrictLogging {

  private def loggerForCompanionInstances: Logger = logger

  /**
    * @param endpoint : Airflow REST API endpoint, aka. http://127.0.0.1:8080/api/experimental
    */
  final case class Airflow(endpoint: String, ingest: String)

  /**
    * datasets in the data pipeline go through several stages and
    * are stored on disk at each of these stages.
    * This setting allow to customize the folder names of each of these stages.
    *
    * @param pending    : Name of the pending area
    * @param unresolved : Named of the unresolved area
    * @param archive    : Name of the archive area
    * @param ingesting  : Name of the ingesting area
    * @param accepted   : Name of the accepted area
    * @param rejected   : Name of the rejected area
    * @param business   : Name of the business area
    */
  final case class Area(
    pending: String,
    unresolved: String,
    archive: String,
    ingesting: String,
    accepted: String,
    rejected: String,
    business: String
  ) {
    val acceptedFinal: String = accepted.toLowerCase(Locale.ROOT)
    val rejectedFinal: String = rejected.toLowerCase(Locale.ROOT)
    val businessFinal: String = business.toLowerCase(Locale.ROOT)
  }

  /**
    * @param options : Map of privacy algorightms name -> PrivacyEngine
    */
  final case class Privacy(options: juMap[String, String])

  final case class Elasticsearch(active: Boolean, options: juMap[String, String])

  /**
    * Configuration for [[com.ebiznext.comet.schema.model.SinkType]]
    *
    * This is used to define an auxiliary output for Audit or Metrics data, in addition to the Parquets
    * The default Index Sink is None, but additional types exists (such as BigQuery or Jdbc)
    */
  @JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS)
  sealed abstract class SinkSettings(val `type`: String) {
    def sinkType: SinkType
  }

  object SinkSettings {

    /**
      * A no-operation Index Output (disabling external output beyond the business area parquets)
      */
    @JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS)
    @JsonDeserialize(builder = classOf[None.NoneBuilder])
    case object None
        extends SinkSettings("None")
        with CometJacksonModule.JacksonProtectedSingleton {
      override def sinkType: SinkType.None.type = SinkType.None

      class NoneBuilder extends CometJacksonModule.ProtectedSingletonBuilder[None.type]
    }

    /**
      * Describes an Index Output delivering values into a BigQuery dataset
      */
    final case class BigQuery(bqDataset: String) extends SinkSettings("BigQuery") {
      override def sinkType: SinkType.BQ.type = SinkType.BQ
    }

    /**
      * Describes an Index Output delivering values into a JDBC-accessible SQL database
      */
    final case class Jdbc(jdbcConnection: String, partitions: Int = 1, batchSize: Int = 1000)
        extends SinkSettings("Jdbc") {
      override def sinkType: SinkType.JDBC.type = SinkType.JDBC
    }
    // TODO: IndexSink has ES, too. Is there a use case for this?
    // Maybe later; additional sink types (e.g. Kafka/Pulsar)?
  }

  /**
    * @param discreteMaxCardinality : Max number of unique values allowed in cardinality compute
    */
  final case class Metrics(
    path: String,
    discreteMaxCardinality: Int,
    active: Boolean,
    index: SinkSettings
  )

  final case class Audit(
    path: String,
    index: SinkSettings,
    maxErrors: Int
  )

  /**
    * Describes a connection to a JDBC-accessible database engine
    *
    * @param uri the URI of the database engine. It must start with "jdbc:"
    * @param user the username under which to connect to the database engine
    * @param password the password to use in order to connect to the database engine
    * @param engineOverride the index into the [[Comet.jdbcEngines]] map of the underlying database engine, in case
    *                       one cannot use the engine name from the uri
    *
    * @note the use case for engineOverride is when you need to have an alternate schema definition
    *       (e.g. non-standard table names) alongside with the regular schema definition, on the same
    *       underlying engine.
    */
  final case class Jdbc(
    uri: String,
    user: String = "",
    password: String = "",
    engineOverride: Option[String] = None
  ) {
    def engine: String = engineOverride.getOrElse(uri.split(':')(1))
  }

  /**
    * Describes how to use a specific type of JDBC-accessible database engine
    *
    * @param driver the qualified class name of the JDBC Driver to use for the specific engine
    * @param tables for each of the Standard Table Names used by Comet, the specific SQL DDL statements as expected
    *               in the engine's own dialect.
    */
  final case class JdbcEngine(
    driver: String,
    tables: scala.collection.Map[String, JdbcEngine.TableDdl]
  )

  object JdbcEngine {

    /**
      * A descriptor of the specific SQL DDL statements required to manage a specific Comet table in a JDBC-accessible
      * database engine
      *
      * @param name the name of the table as it is known on the database engine
      * @param createSql the SQL Create Table statement with the database-specific type, constraints etc. tacked on.
      * @param pingSql a cheap SQL query whose results are irrelevant but guaranteed to trigger an error in case the table is absent
      *
      * @note pingSql is optional, and will default to `select * from `name` where 1=0` as Spark SQL does
      */
    final case class TableDdl(name: String, createSql: String, pingSql: Option[String] = None) {
      def effectivePingSql: String = pingSql.getOrElse(s"select * from $name where 1=0")
    }
  }

  final case class Lock(
    path: String,
    metricsTimeout: Long,
    ingestionTimeout: Long,
    pollTime: FiniteDuration = FiniteDuration(5000L, TimeUnit.MILLISECONDS),
    refreshTime: FiniteDuration = FiniteDuration(5000L, TimeUnit.MILLISECONDS)
  )

  final case class Atlas(uri: String, user: String, password: String, owner: String)

  final case class Internal(cacheStorageLevel: StorageLevel) {}

  /**
    * @param datasets       : Absolute path, datasets root folder beneath which each area is defined.
    * @param metadata       : Absolute path, location where all types / domains and auto jobs are defined
    * @param metrics        : Absolute path, location where all computed metrics are stored
    * @param audit          : Absolute path, location where all log are stored
    * @param archive        : Should we backup the ingested datasets ? true by default
    * @param defaultWriteFormat    : Choose between parquet, orc ... Default is parquet
    * @param launcher       : Cron Job Manager : simple (useful for testing) or airflow ? simple by default
    * @param analyze        : Should we create basics Hive statistics on the generated dataset ? true by default
    * @param hive           : Should we create a Hive Table ? true by default
    * @param area           : see Area above
    * @param airflow        : Airflow end point. Should be defined even if simple launccher is used instead of airflow.
    */
  final case class Comet(
    tmpdir: String,
    jobId: String,
    datasets: String,
    metadata: String,
    metrics: Metrics,
    audit: Audit,
    archive: Boolean,
    lock: Lock,
    defaultWriteFormat: String,
    timestampedCsv: Boolean,
    launcher: String,
    chewerPrefix: String,
    rowValidatorClass: String,
    analyze: Boolean,
    hive: Boolean,
    grouped: Boolean,
    mergeForceDistinct: Boolean,
    area: Area,
    airflow: Airflow,
    elasticsearch: Elasticsearch,
    hadoop: juMap[String, String],
    jdbc: Map[String, Jdbc],
    jdbcEngines: Map[String, JdbcEngine],
    atlas: Atlas,
    privacy: Privacy,
    fileSystem: Option[String],
    metadataFileSystem: Option[String],
    internal: Option[Internal]
  ) extends Serializable {

    @JsonIgnore
    def isElasticsearchSupported(): Boolean = {
      if (
        Version(util.Properties.versionNumberString).compareTo(Version("2.12")) >= 0
        && elasticsearch.active
      ) {
        logger.warn("""Elasticsearch inserts won't be effective before es-hadoop support scala 2.12
            |See https://github.com/elastic/elasticsearch-hadoop/pull/1308
            |""".stripMargin)
        false
      } else true
    }

    val cacheStorageLevel = internal.map(_.cacheStorageLevel).getOrElse(StorageLevel.MEMORY_ONLY)

    @throws(classOf[ObjectStreamException])
    protected def writeReplace: AnyRef = {
      Comet.JsonWrapped(this)
    }
  }

  object Comet {

    private case class JsonWrapped(jsonValue: String) {

      @throws(classOf[ObjectStreamException])
      protected def readResolve: AnyRef = {
        val unwrapped = JsonWrapped.jsonMapper.readValue(jsonValue, classOf[Comet])
        unwrapped
      }
    }

    private object JsonWrapped {
      private def jsonMapper: ObjectMapper = new CometObjectMapper()

      def apply(comet: Comet): JsonWrapped = {
        val writer = jsonMapper.writerFor(classOf[Comet])
        val asJson = writer.writeValueAsString(comet)
        JsonWrapped(asJson)
      }
    }
  }

  private implicit val sinkSettinsConfigs: Configs[SinkSettings] =
    Configs.derive[SinkSettings]
  private implicit val jdbcEngineConfigs: Configs[JdbcEngine] = Configs.derive[JdbcEngine]

  private implicit val storageLevelConfigs: Configs[StorageLevel] =
    Configs[String].map(StorageLevel.fromString).map(_.asInstanceOf[StorageLevel])

  def apply(config: Config): Settings = {
    val jobId = UUID.randomUUID().toString
    val effectiveConfig = config
      .withValue("job-id", ConfigValueFactory.fromAnyRef(jobId, "per JVM instance"))

    val loaded = effectiveConfig.extract[Comet].valueOrThrow { error =>
      error.messages.foreach(err => logger.error(err))
      throw new Exception(s"Failed to load config: $error")
    }
    logger.info(s"Using Config $loaded")
    Settings(loaded, effectiveConfig.getConfig("spark"))
  }

  val cometInputFileNameColumn: String = "comet_input_file_name"

}

/**
  * This class holds the current Comet settings and an assembly of reference instances for core, shared services
  *
  * SMELL: this may be the start of a Dependency Injection root (but at 2-3 objects, is DI justified? probably not
  * quite yet) — cchepelov
  */
final case class Settings(comet: Settings.Comet, sparkConfig: Config) {
  def logger: Logger = Settings.loggerForCompanionInstances

  @transient
  lazy val storageHandler: HdfsStorageHandler = {
    implicit val self: Settings =
      this /* TODO: remove this once HdfsStorageHandler explicitly takes Settings or Settings.Comet in */
    new HdfsStorageHandler(comet.fileSystem)
  }

  @transient
  lazy val metadataStorageHandler: HdfsStorageHandler = {
    implicit val self: Settings =
      this /* TODO: remove this once HdfsStorageHandler explicitly takes Settings or Settings.Comet in */
    new HdfsStorageHandler(comet.metadataFileSystem)
  }

  @transient
  lazy val launcherService: LaunchHandler = comet.launcher match {
    case "simple"  => new SimpleLauncher()
    case "airflow" => new AirflowLauncher()
  }

  /** Publish MDC information into the logging stack.
    *
    * @note this is inherently an effectful operation, which effects global shared mutable state.
    *       It should make little sense to run this code in tests.
    */
  def publishMDCData(): Unit = {
    val rt: RuntimeMXBean = ManagementFactory.getRuntimeMXBean
    MDC.put("PID", rt.getName)
    val oldJobId = Option(MDC.get("JID")).getOrElse(comet.jobId)
    require(
      oldJobId == comet.jobId,
      s"cannot publish different MDC data; a previous jobId ${oldJobId} had been published," +
      s" attempting to reset to ${comet.jobId}"
    )
    MDC.put("JID", comet.jobId)
  }
}
