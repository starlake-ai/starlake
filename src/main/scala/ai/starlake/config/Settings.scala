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

package ai.starlake.config

import ai.starlake.config.Settings.JdbcEngine.TableDdl
import ai.starlake.privacy.PrivacyEngine
import ai.starlake.schema.handlers.{
  AirflowLauncher,
  HdfsStorageHandler,
  LaunchHandler,
  SimpleLauncher
}
import ai.starlake.schema.model.{PrivacyLevel, Sink}
import ai.starlake.utils.{CometObjectMapper, Utils, YamlSerializer}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import pureconfig.ConvertHelpers._
import pureconfig._
import pureconfig.generic.FieldCoproductHint
import pureconfig.generic.auto._

import java.io.ObjectStreamException
import java.util.concurrent.TimeUnit
import java.util.{Locale, Properties, UUID}
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

object Settings extends StrictLogging {

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
    business: String,
    hiveDatabase: String
  ) {
    val acceptedFinal: String = accepted.toLowerCase(Locale.ROOT)
    val rejectedFinal: String = rejected.toLowerCase(Locale.ROOT)
    val businessFinal: String = business.toLowerCase(Locale.ROOT)
    val replayFinal: String = replay.toLowerCase(Locale.ROOT)
  }

  /** @param options
    *   : Map of privacy algorightms name -> PrivacyEngine
    */
  final case class Privacy(options: Map[String, String])

  final case class Elasticsearch(active: Boolean, options: Map[String, String])

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
  final case class JdbcEngine(tables: Map[String, TableDdl])

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
    maxRead: Long = -1,
    fields: List[String] = List("key as STRING", "value as STRING"),
    partitions: Int = 1,
    replicationFactor: Short = 1,
    createOptions: Map[String, String] = Map.empty,
    accessOptions: Map[String, String] = Map.empty
  ) {
    def allAccessOptions(serverProperties: Map[String, String]) = {
      serverProperties ++ accessOptions
    }
  }

  final case class KafkaConfig(
    serverOptions: Map[String, String],
    topics: Map[String, KafkaTopicConfig],
    cometOffsetsMode: Option[String] = Some("STREAM"),
    customDeserializer: Option[String]
  ) {
    lazy val sparkServerOptions: Map[String, String] = {
      val ASSIGN = "assign"
      val SUBSCRIBE_PATTERN = "subscribepattern"
      val SUBSCRIBE = "subscribe"
      val ignoreKafkaProperties = List(
        ConsumerConfig.GROUP_ID_CONFIG,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ASSIGN,
        SUBSCRIBE_PATTERN,
        SUBSCRIBE
      )
      val kafkaServerProperties = new Properties
      serverOptions.foreach { case (k, v) =>
        // for spark we need to prefix them with "kafka."
        kafkaServerProperties.put(k, v)
        if (!ignoreKafkaProperties.contains(k) && !k.startsWith("kafka."))
          kafkaServerProperties.put(s"kafka.$k", v)
      }
      kafkaServerProperties.asScala.toMap
    }
  }

  case class JobScheduling(
    maxJobs: Int,
    poolName: String,
    mode: String,
    file: String
  )

  case class AccessPolicies(apply: Boolean, location: String, projectId: String, taxonomy: String)

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
    * @param defaultFormat
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
    validateOnLoad: Boolean,
    audit: Audit,
    archive: Boolean,
    sinkToFile: Boolean,
    sinkReplayToFile: Boolean,
    lock: Lock,
    defaultFormat: String,
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
    groupedMax: Int,
    mergeForceDistinct: Boolean,
    mergeOptimizePartitionWrite: Boolean,
    area: Area,
    airflow: Airflow,
    elasticsearch: Elasticsearch,
    hadoop: Map[String, String],
    connections: Map[String, Connection],
    jdbcEngines: Map[String, JdbcEngine],
    atlas: Atlas,
    privacy: Privacy,
    fileSystem: String,
    metadataFileSystem: String,
    internal: Option[Internal],
    udfs: Option[String],
    assertions: Assertions,
    kafka: KafkaConfig,
    sqlParameterPattern: String,
    rejectAllOnError: Boolean,
    defaultFileExtensions: String,
    forceFileExtensions: String,
    accessPolicies: AccessPolicies,
    scheduling: JobScheduling,
    maxParCopy: Int,
    dsvOptions: Map[String, String]
  ) extends Serializable {

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
      private def jsonMapper: ObjectMapper = new CometObjectMapper

      def apply(comet: Comet): JsonWrapped = {
        val writer = jsonMapper.writerFor(classOf[Comet])
        val asJson = writer.writeValueAsString(comet)
        JsonWrapped(asJson)
      }
    }
  }

  implicit val sinkHint = new FieldCoproductHint[Sink]("type") {
    override def fieldValue(name: String) = name
  }

  implicit val storageLevelReader =
    ConfigReader.fromString[StorageLevel](catchReadError(StorageLevel.fromString))

  def apply(config: Config): Settings = {

    val jobId = UUID.randomUUID().toString
    val effectiveConfig =
      config.withValue("job-id", ConfigValueFactory.fromAnyRef(jobId, "per JVM instance"))

    val loaded = ConfigSource
      .fromConfig(effectiveConfig)
      .loadOrThrow[Comet]

    logger.info("COMET_FS=" + System.getenv("COMET_FS"))
    logger.info("COMET_ROOT=" + System.getenv("COMET_ROOT"))
    logger.info(YamlSerializer.serializeObject(loaded))
    val settings = Settings(loaded, effectiveConfig.getConfig("spark"))
    val applicationConfPath = new Path(DatasetArea.metadata(settings), "application.conf")
    val result: Settings = if (settings.metadataStorageHandler.exists(applicationConfPath)) {
      val applicationConfContent = settings.metadataStorageHandler.read(applicationConfPath)
      val applicationConfig = ConfigFactory.parseString(applicationConfContent)
      val effectiveApplicationConfig = applicationConfig
        .withFallback(effectiveConfig)
      val mergedSettings = ConfigSource
        .fromConfig(effectiveApplicationConfig)
        .loadOrThrow[Comet]

      Settings(
        mergedSettings,
        effectiveApplicationConfig.getConfig("spark")
      )
    } else
      settings
    val jobConf = initSparkConfig(result)
    result.copy(jobConf = jobConf)
  }

  private def initSparkConfig(settings: Settings): SparkConf = {
    val schedulingConfig = schedulingPath(settings)

    // When using local Spark with remote BigQuery (useful for testing)
    val initialConf =
      sys.env.get("TEMPORARY_GCS_BUCKET") match {
        case Some(value) => new SparkConf.set("temporaryGcsBucket", value)
        case None        => new SparkConf
      }

    val thisConf = settings.sparkConfig
      .entrySet()
      .asScala
      .to[Vector]
      .map(x => (x.getKey, x.getValue.unwrapped().toString))
      .foldLeft(initialConf) { case (conf, (key, value)) => conf.set("spark." + key, value) }
      .set("spark.scheduler.mode", settings.comet.scheduling.mode)

    schedulingConfig.foreach(path => thisConf.set("spark.scheduler.allocation.file", path.toString))

    logger.whenDebugEnabled {
      logger.debug(thisConf.toDebugString)
    }
    thisConf
  }

  private def schedulingPath(settings: Settings): Option[Path] = {
    import settings.comet.scheduling._
    if (file.isEmpty) {
      val schedulingPath = new Path(DatasetArea.metadata(settings), "fairscheduler.xml")
      Some(schedulingPath).filter(settings.metadataStorageHandler.exists)
    } else
      Some(new Path(file))
  }
}

object CometColumns {
  val cometInputFileNameColumn: String = "comet_input_file_name"
  val cometSuccessColumn: String = "comet_success"
  val cometErrorMessageColumn: String = "comet_error_message"
}

/** This class holds the current Comet settings and an assembly of reference instances for core,
  * shared services
  *
  * SMELL: this may be the start of a Dependency Injection root (but at 2-3 objects, is DI
  * justified? probably not quite yet) — cchepelov
  */
final case class Settings(
  comet: Settings.Comet,
  sparkConfig: Config,
  jobConf: SparkConf = new SparkConf
) {

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
    case "simple"  => new SimpleLauncher
    case "airflow" => new AirflowLauncher
  }

}

object PrivacyLevels {
  private def make(schemeName: String, encryptionAlgo: String): (PrivacyEngine, List[String]) = {
    val (privacyObject, typedParams) = PrivacyEngine.parse(encryptionAlgo)
    val encryption = Utils.loadInstance[PrivacyEngine](privacyObject)
    (encryption, typedParams)
  }

  private var allPrivacy = Map.empty[String, ((PrivacyEngine, List[String]), PrivacyLevel)]

  @transient
  def allPrivacyLevels(
    options: Map[String, String]
  ): Map[String, ((PrivacyEngine, List[String]), PrivacyLevel)] = {
    if (allPrivacy.isEmpty) {
      allPrivacy = options.map { case (k, objName) =>
        val encryption = make(k, objName)
        val key = k.toUpperCase(Locale.ROOT)
        (key, (encryption, new PrivacyLevel(key, false)))
      }
    }
    allPrivacy
  }

}
