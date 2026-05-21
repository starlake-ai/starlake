package ai.starlake.job.quack

import ai.starlake.config.Settings
import better.files.{File => BFile}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class QuackState(
  connection: String,
  pid: Long,
  bind: String,
  port: Int,
  logFile: String,
  startedAt: Long
)

object QuackState {

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  def toJson(state: QuackState): String =
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state)

  def fromJson(json: String): QuackState =
    mapper.readValue(json, classOf[QuackState])

  def stateDir(implicit settings: Settings): BFile =
    BFile(new java.net.URI(settings.appConfig.root).getPath) / ".quack"

  def logsDir(implicit settings: Settings): BFile =
    stateDir / "logs"

  def stateFile(connection: String)(implicit settings: Settings): BFile =
    stateDir / s"$connection.json"

  def logFile(connection: String)(implicit settings: Settings): BFile =
    logsDir / s"$connection.log"

  def write(state: QuackState)(implicit settings: Settings): Unit = {
    stateDir.createDirectoryIfNotExists(createParents = true)
    stateFile(state.connection).overwrite(toJson(state))
  }

  def read(connection: String)(implicit settings: Settings): Option[QuackState] = {
    val f = stateFile(connection)
    if (f.exists) Some(fromJson(f.contentAsString)) else None
  }

  def delete(connection: String)(implicit settings: Settings): Unit = {
    val f = stateFile(connection)
    if (f.exists) f.delete()
  }

  /** Hydrate every state file and prune those whose PID is no longer alive. */
  def list()(implicit settings: Settings): List[QuackState] = {
    val dir = stateDir
    if (!dir.exists) Nil
    else {
      dir
        .glob("*.json")
        .toList
        .flatMap { f =>
          val st = fromJson(f.contentAsString)
          if (java.lang.ProcessHandle.of(st.pid).isPresent) Some(st)
          else {
            f.delete()
            None
          }
        }
    }
  }
}