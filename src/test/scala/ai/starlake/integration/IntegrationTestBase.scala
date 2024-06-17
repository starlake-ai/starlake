package ai.starlake.integration

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Directory
import scala.util.Try

class IntegrationTestBase
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with StrictLogging {
  implicit var settings: Settings = Settings(Settings.referenceConfig)

  implicit val copyOptions = File.CopyOptions(overwrite = true)

  val starlakeDir = File(".")
  logger.info(starlakeDir.pathAsString)
  def templates = starlakeDir / "samples"
  def localDir = templates / "spark"
  def incomingDir = localDir / "incoming"
  def sampleDataDir = templates / "sample-data"
  def migrationDir = starlakeDir / "migration"

  override protected def beforeAll() = {
    new Directory(new java.io.File(settings.appConfig.datasets)).deleteRecursively()
  }

  override protected def afterAll() = {
    TestHelper.stopSession()
  }

  override def beforeEach(): Unit = {
    cleanup()
  }

  override def afterEach(): Unit = {
    cleanup()
  }

  def withEnvs[T](envList: Tuple2[String, String]*)(fun: => T): T = {
    val existingValues = envList.flatMap { case (k, _) =>
      Option(System.getenv().get(k)).map(k -> _)
    }
    envList.foreach { case (k, v) => setEnv(k, v) }
    setEnv("SL_INTERNAL_WITH_ENVS_SET", "true")
    Settings.invalidateCaches()
    settings = Settings(Settings.referenceConfig)
    val result = Try {
      fun
    }
    delEnv("SL_INTERNAL_WITH_ENVS_SET")
    envList.foreach { case (k, _) => delEnv(k) }
    existingValues.foreach { case (k, v) => setEnv(k, v) }
    Settings.invalidateCaches()
    result.get
  }

  private def setEnv(key: String, value: String): Unit = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map =
      field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  private def delEnv(key: String): Unit = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map =
      field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.remove(key)
  }

  val directoriesToClear =
    List("incoming", "audit", "datasets", "diagrams", "metadata/dags/generated")

  protected def cleanup(): Unit = {
    cleanup(localDir)
  }

  protected def cleanup(directory: File): Unit = {
    directoriesToClear.foreach { dir =>
      val path = directory / dir
      if (path.exists) {
        path.delete()
      }
    }
  }
  protected def copyFilesToIncomingDir(dir: File): Unit = {
    dir.copyTo(incomingDir)
  }
}
