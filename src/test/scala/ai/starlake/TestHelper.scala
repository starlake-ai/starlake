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

package ai.starlake

import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.AutoJobDesc
import com.dimafeng.testcontainers.{ElasticsearchContainer, KafkaContainer}
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.ingest.LoadConfig
import ai.starlake.schema.handlers.{SchemaHandler, SimpleLauncher, StorageHandler}
import ai.starlake.schema.model.AutoJobDesc
import ai.starlake.utils.{CometObjectMapper, Utils}
import ai.starlake.workflow.{ImportConfig, IngestionWorkflow, WatchConfig}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.github.ghik.silencer.silent
import com.typesafe.config._
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DatasetLogging, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.testcontainers.utility.DockerImageName
import java.io.{File, InputStream}
import java.nio.file.Files
import java.time.LocalDate
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}
import scala.util.Try

trait TestHelper
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with StrictLogging
    with DatasetLogging {

  override protected def afterAll(): Unit = {
    sparkSessionInterest.close()
    super.afterAll()
  }

  private lazy val cometTestPrefix: String = s"comet-test-${TestHelper.runtimeId}"

  private lazy val cometTestInstanceId: String =
    s"${this.getClass.getSimpleName}-${java.util.UUID.randomUUID()}"

  lazy val cometTestId: String = s"${cometTestPrefix}-${cometTestInstanceId}"

  lazy val cometTestRoot: String =
    Option(System.getProperty("os.name")).map(_.toLowerCase contains "windows") match {
      case Some(true) =>
        Files.createTempDirectory(cometTestId).toAbsolutePath.toString.replace("\\", "/")
      case _ => Files.createTempDirectory(cometTestId).toAbsolutePath.toString
    }
  lazy val cometDatasetsPath: String = cometTestRoot + "/datasets"
  lazy val cometMetadataPath: String = cometTestRoot + "/metadata"

  def testConfiguration: Config = {
    val rootConfig = ConfigFactory.parseString(
      s"""
        |COMET_ASSERTIONS_ACTIVE=true
        |COMET_ROOT="${cometTestRoot}"
        |COMET_TEST_ID="${cometTestId}"
        |COMET_DATASETS="${cometDatasetsPath}"
        |COMET_METADATA="${cometMetadataPath}"
        |COMET_TMPDIR="${cometTestRoot}/tmp"
        |COMET_LOCK_PATH="${cometTestRoot}/locks"
        |COMET_METRICS_PATH="${cometTestRoot}/metrics/{domain}/{schema}"
        |COMET_AUDIT_PATH="${cometTestRoot}/audit"
        |COMET_UDFS="ai.starlake.udf.TestUdf"
        |COMET_ACCESS_POLICIES_LOCATION="eu"
        |COMET_ACCESS_POLICIES_TAXONOMY="RGPD"
        |COMET_ACCESS_POLICIES_PROJECT_ID="${sys.env
          .getOrElse("COMET_ACCESS_POLICIES_PROJECT_ID", "invalid_project")}"
        |include required("application-test.conf")
        |""".stripMargin,
      ConfigParseOptions.defaults().setAllowMissing(false)
    )
    val testConfig =
      ConfigFactory
        .load(rootConfig, ConfigResolveOptions.noSystem())
        .withValue(
          "lock.poll-time",
          ConfigValueFactory.fromAnyRef("5 ms")
        ) // in local mode we don't need to wait quite as much as we do on a real cluster

    testConfig
  }

  val allTypes: List[FileToImport] = List(
    FileToImport(
      "default.comet.yml",
      "/sample/default.comet.yml"
    ),
    FileToImport(
      "types.comet.yml",
      "/sample/types.comet.yml"
    )
  )

  val allMappings: List[FileToImport] = List(
    FileToImport(
      "create.ssp",
      "/sample/ddl/bigquery/create.ssp",
      Some("ddl/bigquery")
    )
  )

  val allAssertions: List[FileToImport] = List(
    FileToImport(
      "default.comet.yml",
      "/sample/assertions/default.comet.yml"
    ),
    FileToImport(
      "types.comet.yml",
      "/sample/assertions/assertions.comet.yml"
    )
  )

  val allViews: List[FileToImport] = List(
    FileToImport(
      "default.comet.yml",
      "/sample/views/default.comet.yml"
    ),
    FileToImport(
      "types.comet.yml",
      "/sample/views/views.comet.yml"
    )
  )

  private def readSourceContentAsString(source: Source): String = {
    source.getLines().mkString("\n")
  }

  def loadTextFile(filename: String)(implicit codec: Codec): String = {
    val stream: InputStream = getClass.getResourceAsStream(filename)
    Utils.withResources(Source.fromInputStream(stream))(readSourceContentAsString)
  }

  def loadBinaryFile(filename: String)(implicit codec: Codec): Array[Char] = {
    val stream: InputStream = getClass.getResourceAsStream(filename)
    Iterator
      .continually(stream.read())
      .takeWhile(_ != -1)
      .toArray
      .map(_.toChar)
  }

  def readFileContent(path: String): String =
    Utils.withResources(Source.fromFile(path))(readSourceContentAsString)

  def readFileContent(path: Path): String = readFileContent(path.toUri.getPath)

  def applyTestFileSubstitutions(fileContent: String): String = {
    fileContent.replaceAll("__COMET_TEST_ROOT__", cometTestRoot)
  }

  def withSettings(configuration: Config)(op: Settings => Assertion): Assertion =
    op(Settings(configuration))
  def withSettings(op: Settings => Assertion): Assertion = withSettings(testConfiguration)(op)

  def getResPath(path: String): String = getClass.getResource(path).toURI.getPath

  def prepareSchema(schema: StructType): StructType =
    StructType(schema.fields.filterNot(f => List("year", "month", "day").contains(f.name)))

  def getTodayPartitionPath: String = {
    val now = LocalDate.now
    s"year=${now.getYear}/month=${now.getMonthValue}/day=${now.getDayOfMonth}"
  }

  abstract class WithSettings(configuration: Config = testConfiguration) {
    implicit val settings = Settings(configuration)

    implicit def withSettings: WithSettings = this

    def metadataStorageHandler = settings.storageHandler
    def storageHandler = settings.storageHandler

    @silent val mapper: ObjectMapper with ScalaObjectMapper = {
      val mapper = new CometObjectMapper(new YAMLFactory(), (classOf[Settings], settings) :: Nil)
      mapper
    }
    mapper.setSerializationInclusion(Include.NON_EMPTY)

    def deliverTestFile(importPath: String, targetPath: Path)(implicit codec: Codec): Unit = {
      val content = loadTextFile(importPath)
      val testContent = applyTestFileSubstitutions(content)

      storageHandler.write(testContent, targetPath)

      logger.whenTraceEnabled {
        if (content != testContent) {
          logger.trace(s"delivered ${importPath} to ${targetPath.toString}, WITH substitutions")
        } else {
          logger.trace(s"delivered ${importPath} to ${targetPath.toString}")
        }
      }
    }

    def deliverBinaryFile(importPath: String, targetPath: Path)(implicit codec: Codec): Unit = {
      val content: Array[Char] = loadBinaryFile(importPath)
      storageHandler.writeBinary(content.map(_.toByte), targetPath)
    }

    def cleanMetadata =
      Try {
        val allMetadataFiles = FileUtils
          .listFiles(
            new File(cometMetadataPath),
            TrueFileFilter.INSTANCE,
            TrueFileFilter.INSTANCE
          )
          .asScala

        allMetadataFiles
          .foreach(_.delete())

        deliverTypesFiles()
      }

    def deliverTypesFiles() = {
      allTypes.foreach { typeToImport =>
        val typesPath = new Path(DatasetArea.types, typeToImport.name)
        deliverTestFile(typeToImport.path, typesPath)
      }
      allAssertions.foreach { assertionToImport =>
        val assertionPath = new Path(DatasetArea.assertions, assertionToImport.name)
        deliverTestFile(assertionToImport.path, assertionPath)
      }
      allViews.foreach { viewToImport =>
        val assertionPath = new Path(DatasetArea.views, viewToImport.name)
        deliverTestFile(viewToImport.path, assertionPath)
      }
      allMappings.foreach { mappingToImport =>
        val path = mappingToImport.folder match {
          case None =>
            DatasetArea.mapping.toString
          case Some(folder) =>
            DatasetArea.mapping.toString + "/" + folder
        }
        metadataStorageHandler.mkdirs(new Path(path))
        val mappingPath = new Path(path, mappingToImport.name)
        deliverTestFile(mappingToImport.path, mappingPath)
      }
    }

    // Init
    new File(cometTestRoot).mkdirs()
    new File(cometDatasetsPath).mkdir()
    new File(cometMetadataPath).mkdir()
    new File(cometTestRoot + "/yelp").mkdir()
    new File(cometTestRoot + "/DOMAIN").mkdir()
    new File(cometTestRoot + "/dream").mkdir()
    new File(cometTestRoot + "/json").mkdir()
    new File(cometTestRoot + "/position").mkdir()

    DatasetArea.initMetadata(storageHandler)
    deliverTypesFiles()

  }

  private val sparkSessionInterest = TestHelper.TestSparkSessionInterest()

  lazy val sparkSession = sparkSessionInterest.get

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  abstract class SpecTrait(
    val domainOrJobFilename: String,
    val sourceDomainOrJobPathname: String,
    val datasetDomainName: String,
    val sourceDatasetPathName: String,
    isDomain: Boolean = true // TODO refactor. false if delivering a job
  )(implicit withSettings: WithSettings) {
    implicit def settings: Settings = withSettings.settings

    def storageHandler: StorageHandler = settings.storageHandler
    def metadataStorageHandler: StorageHandler = settings.metadataStorageHandler
    val domainMetadataRootPath: Path = DatasetArea.domains
    val jobMetadataRootPath: Path = DatasetArea.jobs

    def cleanDatasets: Try[Unit] =
      Try {
        val deletedFiles = FileUtils
          .listFiles(
            new File(cometDatasetsPath),
            TrueFileFilter.INSTANCE,
            TrueFileFilter.INSTANCE
          )
          .asScala

        deletedFiles
          .foreach(_.delete())
        if (isDomain)
          deliverSourceDomain()
        else
          deliverSourceJob()
      }

    def deliverSourceDomain(): Unit = {
      val domainPath = new Path(domainMetadataRootPath, domainOrJobFilename)
      withSettings.deliverTestFile(sourceDomainOrJobPathname, domainPath)
    }

    def deliverSourceJob(): Unit = {
      val jobPath = new Path(jobMetadataRootPath, domainOrJobFilename)
      withSettings.deliverTestFile(sourceDomainOrJobPathname, jobPath)

    }

    protected def loadWorkflow()(implicit codec: Codec): IngestionWorkflow = {
      val targetPath = DatasetArea.path(
        DatasetArea.pending(datasetDomainName),
        new Path(sourceDatasetPathName).getName
      )

      withSettings.deliverTestFile(sourceDatasetPathName, targetPath)

      val schemaHandler = new SchemaHandler(settings.storageHandler)
      schemaHandler.checkValidity()

      DatasetArea.initMetadata(metadataStorageHandler)
      DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

      val validator = new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())
      validator
    }

    def loadPending(implicit codec: Codec): Boolean = {
      val validator = loadWorkflow()
      validator.loadPending()
    }

    def secure(config: WatchConfig): Boolean = {
      val validator = loadWorkflow()
      validator.secure(config)
    }

    def load(config: LoadConfig): Boolean = {
      val validator = loadWorkflow()
      validator.load(config)
    }

    def getJobs(): Map[String, AutoJobDesc] = {
      new SchemaHandler(settings.storageHandler).jobs
    }

    def landingPath: String =
      new SchemaHandler(settings.storageHandler)
        .getDomain(datasetDomainName)
        .map(_.resolveDirectory())
        .getOrElse(throw new Exception("Incoming directory must be specified in domain descriptor"))

    def loadLanding(implicit codec: Codec, createAckFile: Boolean = true): Unit = {

      val schemaHandler = new SchemaHandler(settings.storageHandler)

      DatasetArea.initMetadata(metadataStorageHandler)
      DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

      // Get incoming directory from Domain descriptor
      val incomingDirectory = schemaHandler.getDomain(datasetDomainName).map(_.resolveDirectory())
      assert(incomingDirectory.isDefined)

      // Deliver file to incoming folder
      val targetPath = new Path(incomingDirectory.get, new Path(sourceDatasetPathName).getName)
      withSettings.deliverBinaryFile(sourceDatasetPathName, targetPath)
      if (createAckFile) {
        storageHandler.touchz(
          new Path(targetPath.getParent, targetPath.getName.replaceFirst("\\.[^.]+$", ""))
            .suffix(".ack")
        )
      }

      // Load landing file
      val validator = new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())
      validator.loadLanding(ImportConfig())
    }
  }

  def printDF(df: DataFrame, marker: String) = {
    logger.info(s"Displaying schema for $marker")
    logger.info(df.schemaString())
    logger.info(s"Displaying data for $marker")
    logger.info(df.showString(truncate = 0))
    logger.info("-----")
  }

  // https://scala.monster/testcontainers/
  // We need to start it manually because we need to access the HTTP mapped port
  // in the configuration below before any test get executed.
  lazy val kafkaContainer: KafkaContainer = {
    val kafkaDockerImage = "confluentinc/cp-kafka"
    val kafkaDockerTag = "7.1.0"
    val kafkaDockerImageName = DockerImageName.parse(s"$kafkaDockerImage:$kafkaDockerTag")
    KafkaContainer.Def(kafkaDockerImageName).start()
  }

  lazy val esContainer: ElasticsearchContainer = {
    val esDockerImage = "docker.elastic.co/elasticsearch/elasticsearch"
    val esDockerTag = "7.8.1"
    val esDockerImageName = DockerImageName.parse(s"$esDockerImage:$esDockerTag")
    ElasticsearchContainer.Def(esDockerImageName).start()
  }

}

object TestHelper {

  /** This class manages an interest into having an access to the (effectively global) Test
    * SparkSession
    */
  private case class TestSparkSessionInterest() extends AutoCloseable {
    private val closed = new AtomicBoolean(false)

    TestSparkSession.acquire()

    def get: SparkSession = TestSparkSession.get

    def close(): Unit =
      if (!closed.getAndSet(true)) TestSparkSession.release()
  }

  /** This class manages the lifetime of the SparkSession that is shared among various Suites
    * (instances of TestHelper) that may be running concurrently.
    *
    * @note
    *   certain scenarios (such as single-core test execution) can create a window where no
    *   TestSparkSessionInterest() instances exist. In which case, SparkSessions will be closed,
    *   destroyed and rebuilt for each Suite.
    */
  private object TestSparkSession extends StrictLogging {

    /** This state machine manages the lifetime of the (effectively global) [[SparkSession]]
      * instance shared between the Suites that inherit from [[TestHelper]].
      *
      * The allowed transitions allow for:
      *   - registration of interest into having access to the SparkSession
      *   - deferred creation of the SparkSession until there is an actual use
      *   - closure of the SparkSession when there is no longer any expressed interest
      *   - re-start of a fresh SparkSession in case additional Suites spin up after closure of the
      *     SparkSession
      */
    sealed abstract class State {
      def references: Int
      def acquire: State
      def get: (SparkSession, State)
      def release: State
    }

    object State {

      case object Empty extends State {
        def references: Int = 0

        def acquire: State = Latent(1)

        def release: State =
          throw new IllegalStateException(
            "cannot release a Global Spark Session that was never started"
          )

        override def get: (SparkSession, State) =
          throw new IllegalStateException(
            "cannot get global SparkSession without first acquiring a lease to it"
          ) // can we avoid this?
      }

      final case class Latent(references: Int) extends State {
        def acquire: Latent = Latent(references + 1)
        def release: State = if (references > 1) Latent(references - 1) else Empty

        def get: (SparkSession, Running) = {
          val session =
            SparkSession.builder
              .master("local[*]")
              .getOrCreate

          (session, Running(references, session))
        }
      }

      final case class Running(references: Int, session: SparkSession) extends State {
        override def get: (SparkSession, State) = (session, this)

        override def acquire: State = Running(references + 1, session)

        override def release: State =
          if (references > 1) {
            Running(references - 1, session)
          } else {
            session.close()
            Terminated
          }
      }

      case object Terminated extends State {
        override def references: Int = 0

        override def get: (SparkSession, State) =
          throw new IllegalStateException(
            "cannot get new global SparkSession after one was created then closed"
          )

        override def acquire: State = {
          logger.debug(
            "Terminated SparkInterest sees new acquisition — clearing up old closed SparkSession"
          )
          SparkSession.clearActiveSession()
          SparkSession.clearDefaultSession()

          Empty.acquire
        }

        override def release: State =
          throw new IllegalStateException(
            "cannot release again a Global Spark Session after it was already closed"
          )
      }
    }

    private var state: State = State.Empty

    def get: SparkSession =
      this.synchronized {
        val (session, nstate) = state.get
        state = nstate
        logger.trace(s"handing out SparkSession instance, now state=${nstate}")
        session
      }

    def acquire(): Unit =
      this.synchronized {
        val nstate = state.acquire
        logger.trace(s"acquired new interest into SparkSession instance, now state=${nstate}")
        state = nstate
      }

    def release(): Unit =
      this.synchronized {
        val nstate = state.release
        logger.trace(s"released interest from SparkSession instances, now state=${nstate}")
        state = nstate
      }
  }

  private val runtimeId: String = UUID.randomUUID().toString
}

case class FileToImport(name: String, path: String, folder: Option[String] = None)
