package ai.starlake.integration

import ai.starlake.config.Settings
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class IntegrationTestBase
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with StrictLogging {

  implicit val copyOptions = File.CopyOptions(overwrite = true)

  val starlakeDir = File(".")
  logger.info(starlakeDir.pathAsString)
  val templates = starlakeDir / "samples" / "templates"
  val localDir = templates / "spark"
  val sampleDataDir = templates / "sample-data"

  def withEnvs[T](envList: Tuple2[String, String]*)(fun: => T): T = {
    val existingValues = envList.flatMap { case (k, _) =>
      Option(System.getenv().get(k)).map(k -> _)
    }
    envList.foreach { case (k, v) => setEnv(k, v) }
    setEnv("SL_INTERNAL_WITH_ENVS_SET", "true")
    Settings.invalidateCaches()
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

}
