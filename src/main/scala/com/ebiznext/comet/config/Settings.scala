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

import com.ebiznext.comet.privacy.PrivacyEngine
import com.ebiznext.comet.schema.handlers.{
  AirflowLauncher,
  HdfsStorageHandler,
  LaunchHandler,
  SimpleLauncher
}
import com.ebiznext.comet.schema.model.{Mode, PrivacyLevel, Sink}
import com.ebiznext.comet.utils.{CometObjectMapper, Utils, Version}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import configs.Configs
import configs.syntax._
import org.apache.spark.storage.StorageLevel

import java.io.ObjectStreamException
import java.util.concurrent.TimeUnit
import java.util.{Locale, Map => juMap, UUID}
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

object Settings extends StrictLogging {

  private def loggerForCompanionInstances: Logger = logger

  /** @param endpoint
    *   : Airflow REST API endpoint, aka. http://127.0.0.1:8080/api/experimental
    */
  final case class Airflow(endpoint: String, ingest: String)

  /** datasets in the data pipeline go through several stages and are stored on disk at each of
    * these stages. This setting allow to customize the folder names of each of these stages.
    *
    * @param pending
    *   : Name of the pending area
    * @param unresolved
    *   : Named of the unresolved area
    * @param archive
    *   : Name of the archive area
    * @param ingesting
    *   : Name of the ingesting area
    * @param accepted
    *   : Name of the accepted area
    * @param rejected
    *   : Name of the rejected area
    * @param business
    *   : Name of the business area
    */
  final case class Area(
    pending: String,
    unresolved: String,
    archive: String,
    ingesting: String,
    accepted: String,
    rejected: String,
    replay: String,
    business: String
  ) {
    val acceptedFinal: String = accepted.toLowerCase(Locale.ROOT)
    val rejectedFinal: String = rejected.toLowerCase(Locale.ROOT)
    val businessFinal: String = business.toLowerCase(Locale.ROOT)
    val replayFinal: String = replay.toLowerCase(Locale.ROOT)
  }

  /** @param options
    *   : Map of privacy algorightms name -> PrivacyEngine
    */
  final case class Privacy(options: juMap[String, String])

  final case class Elasticsearch(active: Boolean, options: juMap[String, String])

  /** @param discreteMaxCardinality
    *   : Max number of unique values allowed in cardinality compute
    */
  final case class Metrics(
    path: String,
    discreteMaxCardinality: Int,
    active: Boolean,
    sink: Sink
  )

  final case class Assertions(
    path: String,
    active: Boolean,
    sink: Sink
  )

  final case class Audit(
    path: String,
    sink: Sink,
    maxErrors: Int
  )

  /** Describes a connection to a JDBC-accessible database engine
    *
    * @param format
    *   source / sink format (jdbc by default). Cf spark.format possible values
    * @param mode
    *   Spark SaveMode to use. If not present, the save mode will be computed from the write
    *   disposition set in the YAM file
    * @param options
    *   any option required by the format used to ingest / tranform / compute the data. Eg for JDBC
    *   uri, user and password are required uri the URI of the database engine. It must start with
    *   "jdbc:" user the username under which to connect to the database engine password the
    *   password to use in order to connect to the database engine
    * @param engineOverride
    *   the index into the [[Comet.jdbcEngines]] map of the underlying database engine, in case one
    *   cannot use the engine name from the uri
    *
    * @note
    *   the use case for engineOverride is when you need to have an alternate schema definition
    *   (e.g. non-standard table names) alongside with the regular schema definition, on the same
    *   underlying engine.
    */
  final case class Connection(
    format: String = "jdbc",
    mode: Option[String] = None,
    options: Map[String, String] = Map.empty,
    engineOverride: Option[String] = None
  ) {
    def engine: String = engineOverride.getOrElse(options("url").split(':')(1))
  }

  /** Describes how to use a specific type of JDBC-accessible database engine
    *
    * @param tables
    *   for each of the Standard Table Names used by Comet, the specific SQL DDL statements as
    *   expected in the engine's own dialect.
    */
  final case class JdbcEngine(
    tables: scala.collection.Map[String, JdbcEngine.TableDdl]
  )

  object JdbcEngine {

    /** A descriptor of the specific SQL DDL statements required to manage a specific Comet table in
      * a JDBC-accessible database engine
      *
      * @param createSql
      *   the SQL Create Table statement with the database-specific type, constraints etc. tacked
      *   on.
      * @param pingSql
      *   a cheap SQL query whose results are irrelevant but guaranteed to trigger an error in case
      *   the table is absent
      *
      * @note
      *   pingSql is optional, and will default to `select * from `name` where 1=0` as Spark SQL
      *   does
      */
    final case class TableDdl(createSql: String, pingSql: Option[String] = None) {

      def effectivePingSql(tableName: String): String =
        pingSql.getOrElse(s"select count(*) from $tableName where 1=0")
    }
  }

  final case class Lock(
    path: String,
    timeout: Long,
    pollTime: FiniteDuration = FiniteDuration(5000L, TimeUnit.MILLISECONDS),
    refreshTime: FiniteDuration = FiniteDuration(5000L, TimeUnit.MILLISECONDS)
  )

  final case class Atlas(uri: String, user: String, password: String, owner: String)

  final case class Internal(
    cacheStorageLevel: StorageLevel,
    intermediateBigqueryFormat: String = "orc"
  )

  final case class KafkaTopicConfig(
    topicName: String,
    topicOffsetMode: Option[Mode] = Some(Mode.STREAM), // used only for comet_offsets
    maxRead: Long = -1,
    fields: List[String] = List("key as STRING", "value as STRING"),
    partitions: Int = 1,
    replicationFactor: Short = 1,
    writeFormat: String = "parquet",
    createOptions: Map[String, String] = Map.empty,
    accessOptions: Map[String, String] = Map.empty
  )

  final case class KafkaConfig(
    serverOptions: Map[String, String],
    topics: Map[String, KafkaTopicConfig],
    cometOffsetsMode: Option[String] = Some("STREAM")
  )

  /** @param datasets
    *   : Absolute path, datasets root folder beneath which each area is defined.
    * @param metadata
    *   : Absolute path, location where all types / domains and auto jobs are defined
    * @param metrics
    *   : Absolute path, location where all computed metrics are stored
    * @param audit
    *   : Absolute path, location where all log are stored
    * @param archive
    *   : Should we backup the ingested datasets ? true by default
    * @param defaultWriteFormat
    *   : Choose between parquet, orc ... Default is parquet
    * @param defaultRejectedWriteFormat
    *   : Writing format for rejected datasets, choose between parquet, orc ... Default is parquet
    * @param defaultAuditWriteFormat
    *   : Writing format for audit datasets, choose between parquet, orc ... Default is parquet
    * @param launcher
    *   : Cron Job Manager : simple (useful for testing) or airflow ? simple by default
    * @param analyze
    *   : Should we create basics Hive statistics on the generated dataset ? true by default
    * @param hive
    *   : Should we create a Hive Table ? true by default
    * @param area
    *   : see Area above
    * @param airflow
    *   : Airflow end point. Should be defined even if simple launccher is used instead of airflow.
    */
  final case class Comet(
    env: String,
    tmpdir: String,
    datasets: String,
    metadata: String,
    metrics: Metrics,
    audit: Audit,
    archive: Boolean,
    sinkToFile: Boolean,
    sinkReplayToFile: Boolean,
    lock: Lock,
    defaultWriteFormat: String,
    defaultRejectedWriteFormat: String,
    defaultAuditWriteFormat: String,
    csvOutput: Boolean,
    csvOutputExt: String,
    privacyOnly: Boolean,
    launcher: String,
    chewerPrefix: String,
    rowValidatorClass: String,
    treeValidatorClass: String,
    loadStrategyClass: String,
    analyze: Boolean,
    hive: Boolean,
    grouped: Boolean,
    mergeForceDistinct: Boolean,
    area: Area,
    airflow: Airflow,
    elasticsearch: Elasticsearch,
    hadoop: juMap[String, String],
    connections: Map[String, Connection],
    jdbcEngines: Map[String, JdbcEngine],
    atlas: Atlas,
    privacy: Privacy,
    fileSystem: Option[String],
    metadataFileSystem: Option[String],
    internal: Option[Internal],
    udfs: Option[String],
    assertions: Assertions,
    kafka: KafkaConfig,
    sqlParameterPattern: String,
    rejectAllOnError: Boolean
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

    val cacheStorageLevel =
      internal.map(_.cacheStorageLevel).getOrElse(StorageLevel.MEMORY_AND_DISK)

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

  private implicit val sinkConfigs: Configs[Sink] = Configs.derive[Sink]
  private implicit val jdbcEngineConfigs: Configs[JdbcEngine] = Configs.derive[JdbcEngine]

  private implicit val storageLevelConfigs: Configs[StorageLevel] =
    Configs[String].map(StorageLevel.fromString)

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
  val cometSuccessColumn: String = "comet_success"
  val cometErrorMessageColumn: String = "comet_error_message"

  private def make(schemeName: String, encryptionAlgo: String): (PrivacyEngine, List[Any]) = {
    val (privacyObject, typedParams) = PrivacyEngine.parse(encryptionAlgo)
    val encryption = Utils.loadInstance[PrivacyEngine](privacyObject)
    (encryption, typedParams)
  }
}

/** This class holds the current Comet settings and an assembly of reference instances for core,
  * shared services
  *
  * SMELL: this may be the start of a Dependency Injection root (but at 2-3 objects, is DI
  * justified? probably not quite yet) — cchepelov
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

  @transient
  lazy val allPrivacyLevels: Map[String, ((PrivacyEngine, List[Any]), PrivacyLevel)] =
    comet.privacy.options.asScala.map { case (k, objName) =>
      val encryption = Settings.make(k, objName)
      val key = k.toUpperCase(Locale.ROOT)
      (key, (encryption, new PrivacyLevel(key)))
    }.toMap
}
