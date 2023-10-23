package ai.starlake.serve

import ai.starlake.job.Main
import ai.starlake.utils.Utils
import buildinfo.BuildInfo
import com.fasterxml.jackson.databind.ObjectMapper
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.servlet.ServletHandler

import scala.util.Try

object SingleUserMainServer {
  val mapper: ObjectMapper = Utils.newJsonMapper()
  def serve(config: MainServerConfig): Try[Unit] = Try {

    val server = new Server(config.port)
    val handler = new ServletHandler()
    server.setHandler(handler)
    handler.addServletWithMapping(classOf[SingleUserRequestHandler], "/")
    server.start()
  }

  val core = new Main()

  def run(
    root: String,
    metadata: Option[String],
    args: Array[String],
    env: Option[String],
    gcpProject: Option[String]
  ): String = {
    val settings =
      SettingsManager.getUpdatedSettings(root, metadata, env, gcpProject)
    val response = args.head match {
      case "quit" | "exit" =>
        System.exit(0)
        "" // makes the compiler happy
      case "version" => SingleUserMainServer.mapper.writeValueAsString(BuildInfo.version)
      case "reload" =>
        SingleUserMainServer.mapper.writeValueAsString(SettingsManager.reset())
      case "heartbeat" => SingleUserMainServer.mapper.writeValueAsString("OK")
      case "domains" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.domains()(settings))
      case "table-names" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.objectNames()(settings))
      case "jobs" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.jobs()(settings))
      case "types" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.types()(settings))
      case _ =>
        core.run(args)(settings)
        SingleUserMainServer.mapper.writeValueAsString(
          Response(settings.appConfig.rootServe.getOrElse("Should never happen"))
        )
    }
    response
  }
}

case class Response(serve: String)
