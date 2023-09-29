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

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.ingest.{ImportConfig, IngestConfig, LoadConfig}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{Attribute, AutoTaskDesc, Domain}
import ai.starlake.utils.{JobResult, SparkJob, StarlakeObjectMapper, Utils}
import ai.starlake.workflow.IngestionWorkflow
import better.files.{File => BetterFile}
import com.dimafeng.testcontainers.{ElasticsearchContainer, KafkaContainer}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import com.typesafe.config._
import com.typesafe.scalalogging.StrictLogging
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
import scala.annotation.nowarn
import scala.io.{Codec, Source}
import scala.reflect.io.Directory
import scala.util.Try

trait TestHelper
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with StrictLogging
    with DatasetLogging {

  override protected def afterAll(): Unit = {
    sparkSessionInterest.close()
  }

  private lazy val starlakeTestPrefix: String = s"starlake-test-${TestHelper.runtimeId}"

  private def starlakeTestInstanceId: String =
    s"${this.getClass.getSimpleName}-${java.util.UUID.randomUUID()}"

  def starlakeTestId: String = s"${starlakeTestPrefix}-${starlakeTestInstanceId}"

  lazy val starlakeTestRoot: String =
    if (sys.env.getOrElse("SL_INTERNAL_WITH_ENVS_SET", "false").toBoolean) {
      sys.env("SL_ROOT")
    } else {
      Option(System.getProperty("os.name")).map(_.toLowerCase contains "windows") match {
        case Some(true) =>
          BetterFile(Files.createTempDirectory(starlakeTestId)).pathAsString.replace("\\", "/")
        case _ => BetterFile(Files.createTempDirectory(starlakeTestId)).pathAsString
      }
    }
  lazy val starlakeDatasetsPath: String = starlakeTestRoot + "/datasets"
  lazy val starlakeMetadataPath: String = starlakeTestRoot + "/metadata"
  lazy val starlakeLoadPath: String = starlakeMetadataPath + "/load"

  def baseConfigString =
    s"""
       |SL_ASSERTIONS_ACTIVE=true
       |SL_ROOT="${starlakeTestRoot}"
       |SL_TEST_ID="${starlakeTestId}"
       |SL_LOCK_PATH="${starlakeTestRoot}/locks"
       |SL_METRICS_PATH="${starlakeTestRoot}/metrics/{{domain}}/{{schema}}"
       |SL_AUDIT_PATH="${starlakeTestRoot}/audit"
       |SL_UDFS="ai.starlake.udf.TestUdf"
       |TEMPORARY_GCS_BUCKET="${sys.env.getOrElse("TEMPORARY_GCS_BUCKET", "invalid_gcs_bucket")}"
       |SL_ACCESS_POLICIES_LOCATION="eu"
       |SL_ACCESS_POLICIES_TAXONOMY="RGPD"
       |SL_ACCESS_POLICIES_PROJECT_ID="${sys.env
        .getOrElse("SL_ACCESS_POLICIES_PROJECT_ID", "invalid_project")}"
       |include required("application-test.conf")
       |
       |""".stripMargin

  def testConfiguration: Config = {
    val rootConfig = ConfigFactory.parseString(
      baseConfigString,
      ConfigParseOptions.defaults().setAllowMissing(false)
    )
    val testConfig =
      ConfigFactory
        .load(rootConfig) // , ConfigResolveOptions.noSystem())
        .withValue(
          "lock.poll-time",
          ConfigValueFactory.fromAnyRef("5 ms")
        ) // in local mode we don't need to wait quite as much as we do on a real cluster

    testConfig
  }

  val allTypes: List[FileToImport] = List(
    FileToImport(
      "default.sl.yml",
      "/sample/default.sl.yml"
    ),
    FileToImport(
      "types.sl.yml",
      "/sample/types.sl.yml"
    )
  )

  val allExtracts: List[FileToImport] = List(
    FileToImport(
      "create.ssp",
      "/sample/ddl/bigquery/create.ssp",
      Some("ddl/bigquery")
    )
  )

  val allExpectations: List[FileToImport] = List(
    FileToImport(
      "default.sl.yml",
      "/sample/expectations/default.sl.yml"
    ),
    FileToImport(
      "types.sl.yml",
      "/sample/expectations/assertions.sl.yml"
    )
  )

  val allViews: List[FileToImport] = List(
    FileToImport(
      "default.sl.yml",
      "/sample/views/default.sl.yml"
    ),
    FileToImport(
      "types.sl.yml",
      "/sample/views/views.sl.yml"
    )
  )

  val allDags: List[FileToImport] = List(
    FileToImport(
      "sample.sl.yml",
      "/yml2dag//sample.sl.yml"
    ),
    FileToImport(
      "sample.py.j2",
      "/yml2dag/templates/sample.py.j2"
    )
  )

  val applicationYmlConfig: List[FileToImport] = List(
    FileToImport(
      "application.sl.yml",
      "/config//application.sl.yml"
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
    fileContent.replaceAll("__SL_TEST_ROOT__", starlakeTestRoot)
  }

  def withSettings(configuration: Config)(op: Settings => Assertion): Assertion = {
    try {
      op(Settings(configuration))
    } catch {
      case e: Throwable =>
        logger.error("Error in test", e)
        throw e
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

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
    settings.appConfig.connections.values.foreach(_.checkValidity())

    implicit def withSettings: WithSettings = this

    def storageHandler = settings.storageHandler()

    @nowarn val mapper: ObjectMapper with ScalaObjectMapper = {
      val mapper = new StarlakeObjectMapper(new YAMLFactory(), (classOf[Settings], settings) :: Nil)
      mapper
    }

    def deliverTestFile(importPath: String, targetPath: Path)(implicit codec: Codec): Unit = {
      val content = loadTextFile(importPath)
      val testContent = applyTestFileSubstitutions(content)
      storageHandler.mkdirs(targetPath.getParent)
      storageHandler.write(testContent, targetPath)(codec.charSet)

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

    def cleanMetadata: Try[Unit] =
      Try {
        val fload = new File(starlakeLoadPath)
        val dir = new Directory(fload)
        dir.deleteRecursively()
        fload.mkdirs()

        DatasetArea.initMetadata(storageHandler)
        deliverTypesFiles()
      }

    def deliverTypesFiles() = {
      allTypes.foreach { typeToImport =>
        val typesPath = new Path(DatasetArea.types, typeToImport.name)
        deliverTestFile(typeToImport.path, typesPath)
      }
      allExpectations.foreach { assertionToImport =>
        val expectationPath = new Path(DatasetArea.expectations, assertionToImport.name)
        deliverTestFile(assertionToImport.path, expectationPath)
      }
      allViews.foreach { viewToImport =>
        val viewPath = new Path(DatasetArea.views, viewToImport.name)
        deliverTestFile(viewToImport.path, viewPath)
      }
      allExtracts.foreach { extractToImport =>
        val path = extractToImport.folder match {
          case None =>
            DatasetArea.extract.toString
          case Some(folder) =>
            DatasetArea.extract.toString + "/" + folder
        }
        storageHandler.mkdirs(new Path(path))
        val extractPath = new Path(path, extractToImport.name)
        deliverTestFile(extractToImport.path, extractPath)
      }
      allDags.foreach { dagImport =>
        val dagPath = new Path(DatasetArea.dags, dagImport.name)
        deliverTestFile(dagImport.path, dagPath)
      }
    }

    // Init
    new File(starlakeTestRoot).mkdirs()
    new File(starlakeDatasetsPath).mkdir()
    new File(starlakeMetadataPath).mkdir()
    new File(starlakeTestRoot + "/yelp").mkdir()
    new File(starlakeTestRoot + "/DOMAIN").mkdir()
    new File(starlakeTestRoot + "/dream").mkdir()
    new File(starlakeTestRoot + "/json").mkdir()
    new File(starlakeTestRoot + "/position").mkdir()

    DatasetArea.initMetadata(storageHandler)
    deliverTypesFiles()

  }

  private val sparkSessionInterest = TestHelper.TestSparkSessionInterest()

  def sparkSession(implicit settings: Settings) = sparkSessionInterest.get(settings)

  abstract class SpecTrait(
    val sourceDomainOrJobPathname: String,
    val datasetDomainName: String,
    val sourceDatasetPathName: String,
    val jobFilename: Option[String] = None
  )(implicit withSettings: WithSettings) {

    implicit def settings: Settings = withSettings.settings
    def storageHandler: StorageHandler = settings.storageHandler()
    val domainMetadataRootPath: Path = DatasetArea.load
    val jobMetadataRootPath: Path = DatasetArea.transform

    def cleanDatasets: Try[Unit] =
      Try {
        val fDatasets = new File(starlakeDatasetsPath)
        val dir = new Directory(fDatasets)
        dir.deleteRecursively()
        new File(starlakeDatasetsPath).mkdir()

        jobFilename match {
          case Some(_) =>
            deliverSourceJob()
          case None =>
            deliverSourceDomain()
        }
      }

    def deliverSourceDomain(): Unit = {
      deliverSourceDomain(datasetDomainName, sourceDomainOrJobPathname)
    }

    def deliverSourceDomain(datasetDomainName: String, sourceDomainOrJobPathname: String): Unit = {
      val domainPath = new Path(domainMetadataRootPath, s"$datasetDomainName/_config.sl.yml")

      withSettings.deliverTestFile(sourceDomainOrJobPathname, domainPath)
    }

    def deliverSourceJob(): Unit = {
      jobFilename.foreach(
        deliverSourceJob(sourceDomainOrJobPathname, datasetDomainName, _)
      )
    }

    def deliverSourceJob(
      sourceDomainOrJobPathname: String,
      datasetDomainName: String,
      domainOrJobFilename: String
    ): Unit = {
      val jobPath = new Path(jobMetadataRootPath, s"$datasetDomainName/$domainOrJobFilename")
      withSettings.deliverTestFile(sourceDomainOrJobPathname, jobPath)

    }

    protected def loadWorkflow()(implicit codec: Codec): IngestionWorkflow = {
      loadWorkflow(datasetDomainName, sourceDatasetPathName)
    }

    protected def loadWorkflow(datasetDomainName: String, sourceDatasetPathName: String)(implicit
      codec: Codec
    ): IngestionWorkflow = {
      val targetPath = DatasetArea.path(
        DatasetArea.pending(datasetDomainName),
        new Path(sourceDatasetPathName).getName
      )

      withSettings.deliverTestFile(sourceDatasetPathName, targetPath)

      val schemaHandler = new SchemaHandler(settings.storageHandler())
      schemaHandler.checkValidity()

      DatasetArea.initMetadata(storageHandler)
      DatasetArea.initDomains(storageHandler, schemaHandler.domains().map(_.name))

      val validator = new IngestionWorkflow(storageHandler, schemaHandler)
      validator
    }

    def loadPending(implicit codec: Codec): Boolean = {
      val validator = loadWorkflow()
      validator.loadPending()
    }

    def secure(config: LoadConfig): Boolean = {
      val validator = loadWorkflow()
      validator.secure(config)
    }

    def load(config: IngestConfig): Boolean = {
      val validator = loadWorkflow()
      validator.load(config)
    }

    def getTasks(): List[AutoTaskDesc] = {
      new SchemaHandler(settings.storageHandler()).tasks()
    }

    def getDomains(): List[Domain] = {
      new SchemaHandler(settings.storageHandler()).domains()
    }

    def getDomain(domainName: String): Option[Domain] = {
      new SchemaHandler(settings.storageHandler()).getDomain(domainName)
    }

    def landingPath: String =
      new SchemaHandler(settings.storageHandler())
        .getDomain(datasetDomainName)
        .map(_.resolveDirectory())
        .getOrElse(throw new Exception("Incoming directory must be specified in domain descriptor"))

    def loadLanding(implicit codec: Codec, createAckFile: Boolean = true): Unit = {

      val schemaHandler = new SchemaHandler(settings.storageHandler())

      DatasetArea.initMetadata(storageHandler)
      DatasetArea.initDomains(storageHandler, schemaHandler.domains().map(_.name))

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
      val validator = new IngestionWorkflow(storageHandler, schemaHandler)
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
  def deepEquals(l1: List[Attribute], l2: List[Attribute]): Boolean = {
    l1.zip(l2).foreach { case (a1, a2) =>
      a1.name should equal(a2.name)
      a1.`type` should equal(a2.`type`)
      if (a1.`type` == "struct")
        deepEquals(a1.attributes, a2.attributes)
    }
    true
  }
}

object TestHelper {

  /** This class manages an interest into having an access to the (effectively global) Test
    * SparkSession
    */
  private case class TestSparkSessionInterest() extends AutoCloseable {
    private val closed = new AtomicBoolean(false)

    TestSparkSession.acquire()

    def get(settings: Settings): SparkSession = TestSparkSession.get(settings)

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
      def get(settings: Settings): (SparkSession, State)
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

        override def get(settings: Settings): (SparkSession, State) =
          throw new IllegalStateException(
            "cannot get global SparkSession without first acquiring a lease to it"
          ) // can we avoid this?
      }

      final case class Latent(references: Int) extends State {
        def acquire: Latent = Latent(references + 1)
        def release: State = if (references > 1) Latent(references - 1) else Empty

        def get(isettings: Settings): (SparkSession, Running) = {
          logger.info(s"get new SparkSession instance")
          val job = new SparkJob {
            override def name: String = s"test-${UUID.randomUUID()}"

            override implicit def settings: Settings = isettings

            override def run(): Try[JobResult] =
              ??? // we just create a dummy job to get a valid Spark session
          }
          val session = job.session
          (session, Running(references, session))
        }
      }

      final case class Running(references: Int, session: SparkSession) extends State {
        override def get(settings: Settings): (SparkSession, State) = (session, this)

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

        override def get(settings: Settings): (SparkSession, State) =
          throw new IllegalStateException(
            "cannot get new global SparkSession after one was created then closed"
          )

        override def acquire: State = {
          logger.info(
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

    def get(settings: Settings): SparkSession =
      this.synchronized {
        val (session, nstate) = state.get(settings)
        state = nstate
        logger.info(s"handing out SparkSession instance, now state=${nstate}")
        session
      }

    def acquire(): Unit =
      this.synchronized {
        val nstate = state.acquire
        logger.info(s"acquired new interest into SparkSession instance, now state=${nstate}")
        state = nstate
      }

    def release(): Unit =
      this.synchronized {
        val nstate = state.release
        logger.info(s"released interest from SparkSession instances, now state=${nstate}")
        state = nstate
      }
  }

  private val runtimeId: String = UUID.randomUUID().toString
}

case class FileToImport(name: String, path: String, folder: Option[String] = None)
