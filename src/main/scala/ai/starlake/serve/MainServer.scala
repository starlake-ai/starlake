package ai.starlake.serve

import ai.starlake.config.SettingsManager
import ai.starlake.job.Main
import ai.starlake.serve.api.RequestHandler
import buildinfo.BuildInfo
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonSetter, Nulls}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.servlet.ServletHandler

object MainServer {
  val mapper: ObjectMapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper
    .setSerializationInclusion(Include.NON_EMPTY)
    .setDefaultSetterInfo(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY, Nulls.AS_EMPTY))
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def serve(config: MainServerConfig): Unit = {

    val server = new Server(config.port)
    val handler = new ServletHandler()
    server.setHandler(handler)
    handler.addServletWithMapping(classOf[RequestHandler], "/")
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
      case "version"   => MainServer.mapper.writeValueAsString(BuildInfo.version)
      case "heartbeat" => MainServer.mapper.writeValueAsString("OK")
      case "domains" =>
        val settings =
          SettingsManager.getUpdatedSettings(root, metadata, env, gcpProject)
        MainServer.mapper.writeValueAsString(Services.domains()(settings))
      case "jobs" =>
        val settings =
          SettingsManager.getUpdatedSettings(root, metadata, env, gcpProject)
        MainServer.mapper.writeValueAsString(Services.jobs()(settings))
      case "types" =>
        val settings =
          SettingsManager.getUpdatedSettings(root, metadata, env, gcpProject)
        MainServer.mapper.writeValueAsString(Services.types()(settings))
      case _ =>
        val settings =
          SettingsManager.getUpdatedSettings(root, metadata, env, gcpProject)
        core.run(args)(settings)
        MainServer.mapper.writeValueAsString(
          Response(settings.comet.rootServe.getOrElse("Should never happen"))
        )
    }
    response
  }
}

case class Response(serve: String)
