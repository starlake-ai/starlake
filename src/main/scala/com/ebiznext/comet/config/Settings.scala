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
  SchemaHandler,
  SimpleLauncher,
  StorageHandler
}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  JsonDeserializer,
  JsonSerializer,
  ObjectMapper,
  SerializerProvider
}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import configs.syntax._
import org.slf4j.MDC

import scala.concurrent.duration.FiniteDuration

object Settings extends StrictLogging {
  private def loggerForCompanionInstances: Logger = logger
  import java.lang.management.{ManagementFactory, RuntimeMXBean}

  /**
    *
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

  final case class Privacy(options: juMap[String, String])

  final case class Elasticsearch(active: Boolean, options: juMap[String, String])

  /**
    *
    * @param discreteMaxCardinality : Max number of unique values allowed in cardinality compute
    *
    */
  final case class Metrics(
    path: String,
    discreteMaxCardinality: Int,
    active: Boolean,
    index: String,
    options: juMap[String, String]
  )

  final case class Audit(
    path: String,
    active: Boolean,
    index: String,
    options: juMap[String, String],
    maxErrors: Int
  )

  final case class Lock(
    path: String,
    metricsTimeout: Long,
    ingestionTimeout: Long,
    @JsonSerialize(using = classOf[FiniteDurationSerializer])
    @JsonDeserialize(using = classOf[FiniteDurationDeserializer])
    pollTime: FiniteDuration = FiniteDuration(5000L, TimeUnit.MILLISECONDS),
    @JsonSerialize(using = classOf[FiniteDurationSerializer])
    @JsonDeserialize(using = classOf[FiniteDurationDeserializer])
    refreshTime: FiniteDuration = FiniteDuration(5000L, TimeUnit.MILLISECONDS)
  )

  final class FiniteDurationSerializer extends JsonSerializer[FiniteDuration] {
    override def serialize(
      value: FiniteDuration,
      gen: JsonGenerator,
      serializers: SerializerProvider
    ): Unit = {
      gen.writeNumber(value.toMillis)
    }
  }
  final class FiniteDurationDeserializer extends JsonDeserializer[FiniteDuration] {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): FiniteDuration = {
      val milliseconds = ctxt.readValue(p, classOf[Long])
      FiniteDuration.apply(milliseconds, TimeUnit.MILLISECONDS)
    }
  }

  final case class Atlas(uri: String, user: String, password: String, owner: String)

  /**
    *
    * @param datasets    : Absolute path, datasets root folder beneath which each area is defined.
    * @param metadata    : Absolute path, location where all types / domains and auto jobs are defined
    * @param metrics     : Absolute path, location where all computed metrics are stored
    * @param audit       : Absolute path, location where all log are stored
    * @param archive     : Should we backup the ingested datasets ? true by default
    * @param writeFormat : Choose between parquet, orc ... Default is parquet
    * @param launcher    : Cron Job Manager: simple (useful for testing) or airflow ? simple by default
    * @param analyze     : Should we create basics Hive statistics on the generated dataset ? true by default
    * @param hive        : Should we create a Hive Table ? true by default
    * @param area        : see Area above
    * @param airflow     : Airflow end point. Should be defined even if simple launccher is used instead of airflow.
    */
  final case class Comet(
    jobId: String,
    datasets: String,
    metadata: String,
    metrics: Metrics,
    audit: Audit,
    archive: Boolean,
    lock: Lock,
    writeFormat: String,
    launcher: String,
    chewerPrefix: String,
    analyze: Boolean,
    hive: Boolean,
    grouped: Boolean,
    mergeForceDistinct: Boolean,
    area: Area,
    airflow: Airflow,
    elasticsearch: Elasticsearch,
    hadoop: juMap[String, String],
    atlas: Atlas,
    privacy: Privacy,
    fileSystem: Option[String]
  ) extends Serializable {

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
      private def jsonMapper: ObjectMapper = {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        mapper
      }

      def apply(comet: Comet): JsonWrapped = {
        val writer = jsonMapper.writerFor(classOf[Comet])
        val asJson = writer.writeValueAsString(comet)
        JsonWrapped(asJson)
      }
    }
  }

  @deprecated("please use and pass on a Settings instance instead", "2020-02-25") // ready to remove
  def comet(implicit settings: Settings): Comet = settings.comet

  @deprecated("please use and pass on a Settings instance instead", "2020-02-25") // ready to remove
  def storageHandler(implicit settings: Settings): HdfsStorageHandler = settings.storageHandler

  @deprecated("please use and pass on a Settings instance instead", "2020-02-25") // ready to remove
  def schemaHandler(implicit settings: Settings): SchemaHandler = settings.schemaHandler

  def apply(
    config: Config = ConfigFactory
      .load()
  ): Settings = {
    val jobId = UUID.randomUUID().toString
    val effectiveConfig = config
      .withValue("job-id", ConfigValueFactory.fromAnyRef(jobId, "per JVM instance"))

    val loaded = effectiveConfig.extract[Comet].valueOrThrow { error =>
      error.messages.foreach(err => logger.error(err))
      throw new Exception("Failed to load config")
    }
    logger.info(s"Using Config $loaded")
    Settings(loaded)
  }
}

/**
  * This class holds the current Comet settings and an assembly of reference instances for core, shared services
  *
  * SMELL: this may be the start of a Dependency Injection root (but at 2-3 objects, is DI justified? probably not
  * quite yet) — cchepelov
  */
final case class Settings(comet: Settings.Comet) {
  def logger: Logger = Settings.loggerForCompanionInstances

  @transient
  lazy val storageHandler: HdfsStorageHandler = {
    implicit val self
      : Settings = this /* TODO: remove this once HdfsStorageHandler explicitly takes Settings or Settings.Comet in */
    new HdfsStorageHandler(comet.fileSystem)
  }

  @transient
  lazy val schemaHandler: SchemaHandler = {
    implicit val self
      : Settings = this /* TODO: remove this once HdfsStorageHandler explicitly takes Settings or Settings.Comet in */
    new SchemaHandler(storageHandler)
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
