package ai.starlake.serve

import ai.starlake.config.SettingsManager
import ai.starlake.job.Main
import ai.starlake.utils.Utils
import buildinfo.BuildInfo
import com.fasterxml.jackson.databind.ObjectMapper
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.servlet.ServletHandler

object SingleUserMainServer {
  val mapper: ObjectMapper = Utils.newJsonMapper()
  def serve(config: MainServerConfig): Unit = {

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
    val response = args.head match {
      case "quit" | "exit" =>
        System.exit(0)
        "" // makes the compiler happy
      case "version"   => SingleUserMainServer.mapper.writeValueAsString(BuildInfo.version)
      case "heartbeat" => SingleUserMainServer.mapper.writeValueAsString("OK")
      case "domains" =>
        val settings =
          SettingsManager.getUpdatedSettings(root, metadata, env, gcpProject)
        SingleUserMainServer.mapper.writeValueAsString(Services.domains()(settings))
      case "jobs" =>
        val settings =
          SettingsManager.getUpdatedSettings(root, metadata, env, gcpProject)
        SingleUserMainServer.mapper.writeValueAsString(Services.jobs()(settings))
      case "types" =>
        val settings =
          SettingsManager.getUpdatedSettings(root, metadata, env, gcpProject)
        SingleUserMainServer.mapper.writeValueAsString(Services.types()(settings))
      case _ =>
        val settings =
          SettingsManager.getUpdatedSettings(root, metadata, env, gcpProject)
        core.run(args)(settings)
        SingleUserMainServer.mapper.writeValueAsString(
          Response(settings.comet.rootServe.getOrElse("Should never happen"))
        )
    }
    response
  }
}

case class Response(serve: String)
