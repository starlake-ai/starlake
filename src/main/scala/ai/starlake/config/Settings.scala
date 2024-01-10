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
import ai.starlake.job.load.LoadStrategy
import ai.starlake.job.validator.GenericRowValidator
import ai.starlake.privacy.PrivacyEngine
import ai.starlake.schema.handlers._
import ai.starlake.schema.model._
import ai.starlake.utils.{StarlakeObjectMapper, Utils, YamlSerializer}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import pureconfig.ConvertHelpers._
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.generic.{FieldCoproductHint, ProductHint}

import java.io.ObjectStreamException
import java.net.URI
import java.util.concurrent.TimeUnit
import java.util.{Locale, Properties, UUID}
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Try}

object Settings extends StrictLogging {
  private var _referenceConfig: Config = ConfigFactory.load()
  def referenceConfig: Config = _referenceConfig
  private val referenceClassLoader = Thread.currentThread().getContextClassLoader
  def invalidateCaches(): Unit = {
    ConfigFactory.invalidateCaches()
    _referenceConfig = ConfigFactory.load(referenceClassLoader)
  }

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
    active: Boolean
  ) // sinked to audit

  final case class Expectations(
    path: String,
    active: Boolean,
    failOnError: Boolean
  ) // sinked to expectations

  final case class Audit(
    path: String,
    sink: AllSinks,
    maxErrors: Int,
    database: Option[String],
    domain: Option[String],
    active: Option[Boolean],
    sql: Option[String]
  ) {
    def isActive(): Boolean = this.active.getOrElse(false)

    def getConnectionRef()(implicit settings: Settings): String =
      this.sink.connectionRef.getOrElse(settings.appConfig.connectionRef)

    def getConnection()(implicit settings: Settings): Connection =
      settings.appConfig.connections(this.getConnectionRef())

    def getSink()(implicit settings: Settings) =
      this.sink.getSink()

    def getDatabase()(implicit settings: Settings): Option[String] =
      this.database.orElse(settings.appConfig.getDefaultDatabase())

    def getDomain()(implicit settings: Settings): String =
      this.domain.getOrElse("audit")
  }

  /** Describes a connection to a JDBC-accessible database engine
    *
    * @param sparkFormat
    *   source / sink format (jdbc by default). Cf spark.format possible values
    * @param options
    *   any option required by the format used to ingest / tranform / compute the data. Eg for JDBC
    *   uri, user and password are required uri the URI of the database engine. It must start with
    *   "jdbc:" user the username under which to connect to the database engine password the
    *   password to use in order to connect to the database engine
    */
  final case class Connection(
    `type`: Option[String],
    sparkFormat: Option[String] = None,
    quote: Option[String] = None,
    separator: Option[String] = None,
    options: Map[String, String] = Map.empty
  ) {
    override def toString: String = {
      val redactOptions = Utils.redact(options)
      s"""Connection(
         |    type=${`type`},
         |    sparkFormat=$sparkFormat,
         |    quote=$quote,
         |    separator=$separator,
         |    options=$redactOptions
         |)""".stripMargin
    }

    def this() = this(Some(ConnectionType.JDBC.value), None, None, None, Map.empty)

    def checkValidity()(implicit settings: Settings): List[ValidationMessage] = {
      var errors = List.empty[ValidationMessage]
      val defaultTypes = new Path(DatasetArea.types, "default.sl.yml")
      if (!settings.storageHandler().exists(defaultTypes))
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "Types",
          s"File not found: ${defaultTypes.toString}"
        )

      val globalsCometPath = new Path(DatasetArea.metadata, s"env.sl.yml")
      if (!settings.storageHandler().exists(globalsCometPath))
        errors = errors :+ ValidationMessage(
          Severity.Warning,
          "Environment",
          s"env.sl.comet not found in ${globalsCometPath.toString}"
        )

      val tpe = getType()
      tpe match {
        case ConnectionType.JDBC =>
          if (!options.contains("url")) {
            errors = errors :+ ValidationMessage(
              Severity.Error,
              "Connection",
              s"Connection type $tpe requires a url"
            )
          }
        case ConnectionType.BQ =>
          if (!options.contains("location")) {
            errors = errors :+ ValidationMessage(
              Severity.Error,
              "Connection",
              s"Connection type $tpe requires a location"
            )
          }
          if (this.sparkFormat.isDefined) {
            val isIndirectWriteMethod = options.getOrElse("writeMethod", "indirect") == "indirect"
            if (isIndirectWriteMethod && !options.contains("temporaryGcsBucket")) {
              errors = errors :+ ValidationMessage(
                Severity.Warning,
                "Connection",
                s"Connection type $tpe: using gcsBucket as temporaryGcsBucket"
              )
            }
            if (isIndirectWriteMethod && !options.contains("gcsBucket")) {
              errors = errors :+ ValidationMessage(
                Severity.Error,
                "Connection",
                s"Connection type $tpe requires a gcsBucket"
              )
            }
            if (!settings.sparkConfig.hasPath("datasource.bigquery.materializationDataset")) {
              errors = errors :+ ValidationMessage(
                Severity.Error,
                "Connection",
                s"Connection type $tpe requires spark.datasource.bigquery.materializationDataset"
              )
            }
          }

          options.getOrElse("authType", "") match {
            case "APPLICATION_DEFAULT" =>
              if (!options.contains("authScopes")) {
                errors = errors :+ ValidationMessage(
                  Severity.Warning,
                  "Connection",
                  s"authScopes not defined in Connection type $tpe. Using 'https://www.googleapis.com/auth/cloud-platform'"
                )
              }
            case "SERVICE_ACCOUNT_JSON_KEYFILE" =>
              if (!options.contains("jsonKeyfile")) {
                errors = errors :+ ValidationMessage(
                  Severity.Error,
                  "Connection",
                  s"Connection type $tpe requires a jsonKeyfile"
                )
              }
            case "USER_CREDENTIALS" =>
              val clientId = options.get("clientId")
              val clientSecret = options.get("clientSecret")
              val refreshToken = options.get("refreshToken")
              if (clientId.isEmpty || clientSecret.isEmpty || refreshToken.isEmpty) {
                errors = errors :+ ValidationMessage(
                  Severity.Error,
                  "Connection",
                  s"Connection type $tpe requires a clientId, clientSecret and refreshToken options"
                )
              }
            case "ACCESS_TOKEN" =>
              val accessToken = options.get("gcpAccessToken")
              if (accessToken.isEmpty) {
                errors = errors :+ ValidationMessage(
                  Severity.Error,
                  "Connection",
                  s"Connection type $tpe requires a gcpAccessToken option"
                )
              }
            case _ =>
              errors = errors :+ ValidationMessage(
                Severity.Error,
                "Connection",
                s"Connection type $tpe requires an authType"
              )
          }
        case _ =>
      }
      errors
    }

    /** The engine is Spark when sparkFormat is defined or when the connection type is not bigquery
      * @return
      *   the engine Spark or Bigquery only
      */
    def getEngine(): Engine = {
      if (sparkFormat.isDefined) Engine.SPARK
      else {
        val tpe = getType()
        tpe match {
          case ConnectionType.BQ   => Engine.BQ
          case ConnectionType.JDBC => Engine.JDBC
          case _                   => Engine.SPARK
        }
      }
    }

    def getType(): ConnectionType = {
      val tpe = `type`
        .orElse {
          val isJDBC = options.get("url").exists(_.startsWith("jdbc:"))
          if (isJDBC) Some("jdbc") else None
        }
        .getOrElse {
          throw new Exception(s"Connection type not found for options $options")
        }
      ConnectionType.fromString(tpe)
    }

    def datawareOptions(): Map[String, String] =
      options.filterKeys(!Connection.allstorageOptions.contains(_))

    def authOptions(): Map[String, String] =
      options.filterKeys(Connection.allstorageOptions.contains(_))

    def getJdbcEngineName(): Engine = {
      val engineName = sparkFormat match {
        case None | Some("jdbc") =>
          this.`type`.getOrElse(throw new Exception("Should never happen")) match {
            case "jdbc" =>
              val engineName = options("url").split(':')(1).toLowerCase()
              if (engineName == "databricks")
                "spark"
              else engineName
            case "bigquery" | "bq" => "bigquery"
            case _                 =>
              // if this is a jdbc url (aka snowflake, redshift ...)
              options
                .get("url")
                .map(_.split(':')(1))
                .getOrElse("spark")
          }
        case Some(_) =>
          // if this is a jdbc url (aka snowflake, redshift ...)
          options
            .get("url")
            .map(_.split(':')(1))
            .getOrElse("spark")

      }
      Engine.fromString(engineName)
    }

    def isSnowflake(): Boolean = getJdbcEngineName().toString == "snowflake"
    def isRedshift(): Boolean = getJdbcEngineName().toString == "redshift"
  }

  object Connection {
    val gcsOptions = List(
      "gcsBucket",
      "temporaryGcsBucket",
      "authType",
      "jsonKeyfile",
      "clientId",
      "clientSecret",
      "refreshToken"
    )
    val azureOptions = List(
      "azureStorageContainer",
      "azureStorageAccount",
      "azureStorageKey"
    )
    val s3Options = Nil

    val allstorageOptions = gcsOptions ++ azureOptions ++ s3Options
  }
  final case class Connections(connections: Map[String, Connection] = Map.empty)

  /** Describes how to use a specific type of JDBC-accessible database engine
    *
    * @param tables
    *   for each of the Standard Table Names used by Comet, the specific SQL DDL statements as
    *   expected in the engine's own dialect.
    */
  final case class JdbcEngine(
    tables: Map[String, TableDdl],
    canMerge: Boolean,
    quote: String,
    preactions: String
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
      * @note
      *   pingSql is optional, and will default to `select * from `name` where 1=0` as Spark SQL
      *   does
      */
    final case class TableDdl(
      createSql: String,
      pingSql: Option[String],
      selectSql: Option[String] = None
    ) {

      def effectivePingSql(tableName: String): String =
        pingSql.getOrElse(s"select count(*) from $tableName where 1=0")
    }
  }

  final case class Http(
    interface: String,
    port: Int
  )

  final case class Lock(
    path: String,
    timeout: Long,
    pollTime: FiniteDuration = FiniteDuration(5000L, TimeUnit.MILLISECONDS),
    refreshTime: FiniteDuration = FiniteDuration(5000L, TimeUnit.MILLISECONDS)
  )

  final case class Internal(
    cacheStorageLevel: StorageLevel,
    intermediateBigqueryFormat: String,
    temporaryGcsBucket: Option[String],
    substituteVars: Boolean = true,
    bqAuditSaveInBatchMode: Boolean = true
  )

  final case class KafkaTopicConfig(
    topicName: String,
    maxRead: Long = -1,
    fields: List[String] = List("key as STRING", "value as STRING"),
    partitions: Int = 1,
    replicationFactor: Short = 1,
    createOptions: Map[String, String] = Map.empty,
    accessOptions: Map[String, String] = Map.empty,
    headers: Map[String, Map[String, String]] = Map.empty
  ) {
    def allAccessOptions()(implicit settings: Settings): Map[String, String] = {
      settings.appConfig.kafka.sparkServerOptions ++ accessOptions
    }
  }

  final case class KafkaConfig(
    serverOptions: Map[String, String],
    topics: Map[String, KafkaTopicConfig],
    cometOffsetsMode: Option[String] = Some("STREAM"),
    customDeserializers: Option[Map[String, String]]
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
      val kafkaServerProperties = new Properties()
      serverOptions.foreach { case (k, v) =>
        // for spark we need to prefix them with "kafka."
        kafkaServerProperties.put(k, v)
        if (!ignoreKafkaProperties.contains(k) && !k.startsWith("kafka."))
          kafkaServerProperties.put(s"kafka.$k", v)
      }
      kafkaServerProperties.asScala.toMap
    }
  }

  case class SparkScheduling(
    maxJobs: Int,
    poolName: String,
    mode: String,
    file: String
  )

  case class DagRef(load: Option[String], transform: Option[String])

  case class AccessPolicies(apply: Boolean, location: String, database: String, taxonomy: String)

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
    * @param analyze
    *   : Should we create basics Hive statistics on the generated dataset ? true by default
    * @param hive
    *   : Should we create a Hive Table ? true by default
    * @param area
    *   : see Area above
    */
  final case class AppConfig(
    env: String,
    datasets: String,
    dags: String,
    metadata: String,
    metrics: Metrics,
    validateOnLoad: Boolean,
    audit: Audit,
    archive: Boolean,
    sinkReplayToFile: Boolean,
    lock: Lock,
    defaultWriteFormat: String,
    defaultRejectedWriteFormat: String,
    defaultAuditWriteFormat: String,
    csvOutput: Boolean,
    csvOutputExt: String,
    privacyOnly: Boolean,
    emptyIsNull: Boolean,
    loader: String,
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
    hadoop: Map[String, String],
    connections: Map[String, Connection],
    jdbcEngines: Map[String, JdbcEngine],
    privacy: Privacy,
    root: String,
    internal: Option[Internal],
    accessPolicies: AccessPolicies,
    sparkScheduling: SparkScheduling,
    udfs: Option[String],
    expectations: Expectations,
    sqlParameterPattern: String,
    rejectAllOnError: Boolean,
    rejectMaxRecords: Int,
    maxParCopy: Int,
    kafka: KafkaConfig, // not in schemastore yet
    dsvOptions: Map[String, String],
    rootServe: Option[String],
    forceViewPattern: String,
    forceDomainPattern: String,
    forceTablePattern: String,
    forceJobPattern: String,
    forceTaskPattern: String,
    useLocalFileSystem: Boolean,
    sessionDurationServe: Long,
    database: String,
    tenant: String,
    connectionRef: String,
    schedulePresets: Map[String, String],
    maxParTask: Int,
    refs: List[Ref],
    dagRef: Option[DagRef],
    forceHalt: Boolean,
    jobIdEnvName: Option[String],
    archiveTablePattern: String,
    archiveTable: Boolean,
    version: String,
    autoExportSchema: Boolean,
    longJobTimeoutMs: Long,
    shortJobTimeoutMs: Long,
    createSchemaIfNotExists: Boolean,
    http: Http
    // createTableIfNotExists: Boolean
  ) extends Serializable {

    def getUdfs(): Seq[String] =
      udfs
        .map { udfs =>
          udfs.split(',').toList
        }
        .getOrElse(Nil)
        .filter(_.nonEmpty)

    @JsonIgnore
    lazy val fileSystem: String = {
      val protocolSeperator = "://"
      if (root.matches("^\\w+?:\\/\\/.*")) { // check if it follows URI pattern
        val uri = new URI(root)
        uri.getScheme match {
          case "file" => uri.getScheme + protocolSeperator
          case scheme =>
            // get bucket name
            val bucketName =
              root.substring(scheme.length + protocolSeperator.length).takeWhile(_ != '/')
            s"$scheme$protocolSeperator$bucketName"
        }
      } else {
        s"file$protocolSeperator"
      }
    }

    @JsonIgnore
    def getDefaultDatabase(): Option[String] = if (database.isEmpty) None else Some(database)

    val cacheStorageLevel: StorageLevel =
      internal.map(_.cacheStorageLevel).getOrElse(StorageLevel.MEMORY_AND_DISK)

    @JsonIgnore
    def isHiveCompatible(): Boolean = {
      val connectionTypeIsHive = this.connections
        .get(this.connectionRef)
        .exists { conn =>
          conn.`type`.getOrElse("").toLowerCase() == "hive"
        }
      connectionTypeIsHive || hive || Utils.isRunningInDatabricks()
    }

    @JsonIgnore
    def connection(name: String): Option[Connection] = connections.get(name)

    @JsonIgnore
    def connectionOptions(name: String): Map[String, String] =
      connections(name).options

  }

  object AppConfig {
    def checkValidity(
      storageHandler: StorageHandler,
      settings: Settings
    ): List[ValidationMessage] = {
      var errors = List.empty[ValidationMessage]
      val appConfig = settings.appConfig
      if (appConfig.env.nonEmpty) {
        val envFile = new Path(DatasetArea.metadata(settings), "env." + appConfig.env + ".sl.yml")
        if (!storageHandler.exists(envFile)) {
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"env.${appConfig.env}.sl.yml not found in ${envFile.toString}"
          )
        }
      }
      Try {
        Utils
          .loadInstance[GenericRowValidator](settings.appConfig.rowValidatorClass)
      } match {
        case scala.util.Failure(exception) =>
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"rowValidatorClass ${settings.appConfig.rowValidatorClass} not found"
          )
        case _ =>
      }

      Try {
        Utils
          .loadInstance[GenericRowValidator](settings.appConfig.treeValidatorClass)
      } match {
        case scala.util.Failure(exception) =>
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"treeValidatorClass ${settings.appConfig.treeValidatorClass} not found"
          )
        case _ =>
      }
      Try {
        Utils
          .loadInstance[LoadStrategy](settings.appConfig.loadStrategyClass)
      } match {
        case scala.util.Failure(exception) =>
          exception.printStackTrace()
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"loadStrategyClass ${settings.appConfig.loadStrategyClass} not found"
          )
        case _ =>
      }

      if (!Set("spark", "native").contains(settings.appConfig.loader)) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"loader ${settings.appConfig.loader} not supported"
        )
      }
      settings.appConfig.connections.foreach { case (name, connection) =>
        errors = errors ++ connection.checkValidity()(settings)
      }
      val path = new Path(settings.appConfig.root)
      if (!storageHandler.exists(path)) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"root ${settings.appConfig.root} not found"
        )
      }
      Try {
        settings.appConfig.sqlParameterPattern.r
      } match {
        case scala.util.Failure(exception) =>
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"sqlParameterPattern ${settings.appConfig.sqlParameterPattern} is not a valid regex"
          )
        case _ =>
      }
      if (settings.appConfig.rejectMaxRecords < 0) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"rejectMaxRecords ${settings.appConfig.rejectMaxRecords} must be positive"
        )
      }
      if (settings.appConfig.maxParCopy <= 0) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"maxParCopy ${settings.appConfig.maxParCopy} must be positive"
        )
      }
      val patterns = List(
        (settings.appConfig.forceViewPattern, "forceViewPattern"),
        (settings.appConfig.forceDomainPattern, "forceDomainPattern"),
        (settings.appConfig.forceTablePattern, "forceTablePattern"),
        (settings.appConfig.forceJobPattern, "forceJobPattern"),
        settings.appConfig.forceTaskPattern -> "forceTaskPattern"
      )
      patterns.foreach { case (value, name) =>
        Try {
          value.r
        } match {
          case Failure(exception) =>
            errors = errors :+ ValidationMessage(
              Severity.Error,
              "AppConfig",
              s"$name value is not a valid regex"
            )
          case _ =>
        }
      }
      if (settings.appConfig.sessionDurationServe <= 0) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"sessionDurationServe ${settings.appConfig.sessionDurationServe} must be positive"
        )
      }

      val validConnectionNames = settings.appConfig.connections.keys.mkString(", ")
      if (settings.appConfig.connectionRef.isEmpty) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"connectionRef must be defined. Valid connection names are $validConnectionNames"
        )
      } else {
        settings.appConfig.connections.get(settings.appConfig.connectionRef) match {
          case Some(_) =>
          case None =>
            errors = errors :+ ValidationMessage(
              Severity.Error,
              "AppConfig",
              s"Connection ${settings.appConfig.connectionRef} not found. Valid connection names are $validConnectionNames"
            )
        }
      }

      settings.appConfig.schedulePresets.foreach { case (name, cron) =>
        Try {
          cron.r
        } match {
          case Failure(exception) =>
            errors = errors :+ ValidationMessage(
              Severity.Error,
              "AppConfig",
              s"schedulePresets $name value is not a valid cron"
            )
          case _ =>
        }
      }

      val dagRef: List[String] =
        settings.appConfig.dagRef
          .map(ref => List(ref.load.toList, ref.transform.toList).flatten)
          .getOrElse(Nil)

      dagRef.foreach { dagConfigRef =>
        val dagConfigPath = new Path(DatasetArea.dags(settings), dagConfigRef)
        if (!storageHandler.exists(dagConfigPath)) {
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"dagConfigRef $dagConfigRef not found in ${dagConfigPath.toString}"
          )
        }
      }
      errors
    }

    private case class JsonWrapped(jsonValue: String) {

      @throws(classOf[ObjectStreamException])
      protected def readResolve: AnyRef = {
        val unwrapped = JsonWrapped.jsonMapper.readValue(jsonValue, classOf[AppConfig])
        unwrapped
      }
    }

    private object JsonWrapped {
      private def jsonMapper: ObjectMapper = new StarlakeObjectMapper()

      def apply(comet: AppConfig): JsonWrapped = {
        val writer = jsonMapper.writerFor(classOf[AppConfig])
        val asJson = writer.writeValueAsString(comet)
        JsonWrapped(asJson)
      }
    }
  }

  implicit val sinkHint: FieldCoproductHint[Sink] = new FieldCoproductHint[Sink]("type") {
    override def fieldValue(name: String) = name
  }

  implicit val storageLevelReader: ConfigReader[StorageLevel] =
    ConfigReader.fromString[StorageLevel](catchReadError(StorageLevel.fromString))

  /** @param config
    *   : usually the default configuration loaded from reference.conf except in tests
    * @return
    *   final configuration after merging with application.conf & application. sl.yml
    */
  def apply(config: Config): Settings = {
    val jobId = UUID.randomUUID().toString
    val effectiveConfig =
      config.withValue("job-id", ConfigValueFactory.fromAnyRef(jobId, "per JVM instance"))

    // Load reference.conf
    val loaded = {
      implicit def hint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))
      ConfigSource
        .fromConfig(effectiveConfig)
        .loadOrThrow[AppConfig]
    }

    logger.info(
      "ENV SL_ROOT=" + Option(System.getenv("SL_ROOT")).getOrElse("")
    )
    logger.debug(YamlSerializer.serializeObject(loaded))
    val settings =
      Settings(loaded, effectiveConfig.getConfig("spark"), effectiveConfig.getConfig("extra"))

    // Load application.conf / application.sl.yml
    val applicationConfSettings =
      loadApplicationYaml(effectiveConfig, settings)
        .orElse(loadApplicationConf(effectiveConfig, settings))
        .getOrElse(settings)

    applicationConfSettings.storageHandler(true) // Reload with the authentication settings

    // Load fairscheduler.xml
    val jobConf = initSparkSchedulingConfig(applicationConfSettings)
    val withSparkConfig = applicationConfSettings.copy(jobConf = jobConf)
    val withDefaultSchdules = addDefaultSchedules(withSparkConfig)
    withDefaultSchdules
  }

  val defaultCronPresets = Map(
    "hourly"  -> "0 * * * *",
    "daily"   -> "0 0 * * *",
    "weekly"  -> "0 0 * * 1",
    "monthly" -> "0 0 1 * *",
    "yearly"  -> "0 0 1 1 *"
  )

  def addDefaultSchedules(settings: Settings): Settings = {
    val schedules = settings.appConfig.schedulePresets ++ defaultCronPresets
    settings.copy(appConfig = settings.appConfig.copy(schedulePresets = schedules))
  }

  private def loadApplicationConf(effectiveConfig: Config, settings: Settings): Option[Settings] = {
    val applicationConfPath = new Path(DatasetArea.metadata(settings), "application.conf")
    if (settings.storageHandler().exists(applicationConfPath)) {
      val applicationConfContent = settings.storageHandler().read(applicationConfPath)
      val applicationConfig = ConfigFactory.parseString(applicationConfContent).resolve()
      val effectiveApplicationConfig = applicationConfig
        .withFallback(effectiveConfig)
      logger.debug(effectiveApplicationConfig.toString)
      val mergedSettings = ConfigSource
        .fromConfig(effectiveApplicationConfig)
        .loadOrThrow[AppConfig]

      Some(
        Settings(
          mergedSettings,
          effectiveApplicationConfig.getConfig("spark"),
          effectiveApplicationConfig.getConfig("extra")
        )
      )
    } else {
      None
    }
  }

  /** Load application.sl.yml from metadata folder
    * @param effectiveConfig:
    *   config to merge with application.sl.yml
    * @param settings
    *   :
    * @return
    */
  private def loadApplicationYaml(effectiveConfig: Config, settings: Settings): Option[Settings] = {
    val applicationYml = Option("application.sl.yml").find { filename =>
      val applicationYmlPath = new Path(DatasetArea.metadata(settings), filename)
      settings.storageHandler().exists(applicationYmlPath)
    }
    val applicationYmlConfig = applicationYml match {
      case Some(filename) =>
        val schemaHandler = new SchemaHandler(settings.storageHandler())(settings)
        val applicationYmlPath = new Path(DatasetArea.metadata(settings), filename)
        val applicationYmlContent = settings.storageHandler().read(applicationYmlPath)
        val content =
          Utils.parseJinja(applicationYmlContent, schemaHandler.activeEnvVars())(settings)
        val jsonNode: JsonNode = YamlSerializer.mapper.readTree(content)
        // application: root node is optional
        val appNode = jsonNode.path("application")
        val finalNode =
          if (appNode.isNull() || appNode.isMissingNode) {
            jsonNode
          } else
            appNode

        val jsonString = Utils.newJsonMapper().writeValueAsString(finalNode)
        val applicationConfig = ConfigFactory.parseString(jsonString).resolve()
        Some(applicationConfig)
      case None =>
        None
    }

    val applicationSettings = applicationYmlConfig match {
      case Some(applicationConfig) =>
        val effectiveApplicationConfig = applicationConfig
          .withFallback(effectiveConfig)
        logger.debug(effectiveApplicationConfig.toString)

        val mergedSettings = {
          implicit def hint[A]: ProductHint[A] =
            ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))
          ConfigSource
            .fromConfig(effectiveApplicationConfig)
            .loadOrThrow[AppConfig]
        }

        val applicationSettings = Settings(
          mergedSettings,
          effectiveApplicationConfig.getConfig("spark"),
          effectiveApplicationConfig.getConfig("extra")
        )
        Some(applicationSettings)
      case None =>
        None
    }
    applicationSettings.map(_.storageHandler(true)) // Reload with the authentication settings
    applicationSettings
  }

  private def initSparkSchedulingConfig(settings: Settings): SparkConf = {
    val schedulingConfig = schedulingPath(settings)

    // When using local Spark with remote BigQuery (useful for testing)
    val initialConf =
      settings.appConfig.internal.flatMap(_.temporaryGcsBucket) match {
        case Some(value) => new SparkConf().set("temporaryGcsBucket", value)
        case None        => new SparkConf()
      }

    val thisConf = settings.sparkConfig
      .entrySet()
      .asScala
      .toVector
      .map(x => (x.getKey, x.getValue.unwrapped().toString))
      .foldLeft(initialConf) { case (conf, (key, value)) =>
        logger.debug(s"Setting key: ${key}")
        conf.set("spark." + key, value)
      }
      .set("spark.scheduler.mode", settings.appConfig.sparkScheduling.mode)

    schedulingConfig.foreach(path => thisConf.set("spark.scheduler.allocation.file", path.toString))

    logger.whenDebugEnabled {
      logger.debug(thisConf.toDebugString)
    }
    thisConf
  }

  private def schedulingPath(settings: Settings): Option[Path] = {
    import settings.appConfig.sparkScheduling._
    if (file.isEmpty) {
      val schedulingPath = new Path(DatasetArea.metadata(settings), "fairscheduler.xml")
      Some(schedulingPath).filter(settings.storageHandler().exists)
    } else
      Some(new Path(file))
  }
}

object CometColumns {
  val cometInputFileNameColumn: String = "comet_input_file_name"
  val slSuccessColumn: String = "sl_success"
  val slErrorMessageColumn: String = "sl_error_message"
}

/** This class holds the current Comet settings and an assembly of reference instances for core,
  * shared services
  *
  * SMELL: this may be the start of a Dependency Injection root (but at 2-3 objects, is DI
  * justified? probably not quite yet) — cchepelov
  */
final case class Settings(
  appConfig: Settings.AppConfig,
  sparkConfig: Config,
  extraConf: Config,
  jobConf: SparkConf = new SparkConf()
) {

  var _storageHandler: Option[StorageHandler] = None

  @transient
  def storageHandler(reload: Boolean = false): StorageHandler = {
    _storageHandler match {
      case Some(handler) if !reload => handler
      case _ =>
        implicit val self: Settings = this
        val handler = new HdfsStorageHandler(appConfig.fileSystem)
        _storageHandler = Some(handler)
        handler
    }
  }

}

object PrivacyLevels {
  private def make(schemeName: String, encryptionAlgo: String): (PrivacyEngine, List[String]) = {
    val (privacyObject, typedParams) = PrivacyEngine.parse(encryptionAlgo)
    val encryption = Utils.loadInstance[PrivacyEngine](privacyObject)
    (encryption, typedParams)
  }

  private var allPrivacy = Map.empty[String, ((PrivacyEngine, List[String]), PrivacyLevel)]

  def resetAllPrivacy(): Unit =
    allPrivacy = Map.empty[String, ((PrivacyEngine, List[String]), PrivacyLevel)]

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
