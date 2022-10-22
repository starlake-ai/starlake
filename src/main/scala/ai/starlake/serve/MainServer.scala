package ai.starlake.serve

import ai.starlake.config.Settings
import ai.starlake.job.Main
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import ai.starlake.serve.api.RequestHandler
import better.files.File
import buildinfo.BuildInfo
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.ConfigFactory
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.servlet.ServletHandler

object MainServer {
  val mapper: ObjectMapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(Include.NON_EMPTY)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def serve(config: MainServerConfig): Unit = {
    val server = new Server(config.port)
    val handler = new ServletHandler()
    server.setHandler(handler)
    handler.addServletWithMapping(classOf[RequestHandler], "/")
    server.start()
  }
  private val settingsMap: scala.collection.mutable.Map[String, Settings] =
    scala.collection.mutable.Map.empty

  val core = new Main()

  private def getUpdatedSettings(
    root: String,
    metadata: Option[String],
    env: Option[String],
    gcpProject: Option[String]
  ): Settings = {
    val sysProps = System.getProperties()
    val outFile = File(root, "out")
    outFile.createDirectoryIfNotExists()

    sysProps.setProperty("root-serve", outFile.pathAsString)

    gcpProject.foreach { gcpProject =>
      val oldGcpProject =
        Option(sysProps.getProperty("GOOGLE_CLOUD_PROJECT")).getOrElse("")
      if (oldGcpProject != gcpProject) {
        sysProps.setProperty("GOOGLE_CLOUD_PROJECT", gcpProject)
        BigQueryJobBase.bigquery(reload = true)
      }
    }

    env.foreach { env =>
      val oldEnv = Option(sysProps.getProperty("env")).getOrElse("")
      if (oldEnv != env) settingsMap.clear()
    }

    metadata.foreach { metadata =>
      val oldMetadata = Option(sysProps.getProperty("metadata")).getOrElse("")
      if (oldMetadata != metadata) settingsMap.clear()
    }
    settingsMap.getOrElse(
      root, {
        settingsMap.clear() // For now we keep only one project in memory
        sysProps.setProperty("root", root)
        sysProps.setProperty("metadata", root + "/" + metadata.getOrElse("metadata"))
        env match {
          case Some(env) if env.nonEmpty && env != "None" =>
            sysProps.setProperty("env", env)
          case _ =>
            sysProps.setProperty("env", "prod") // prod is the default value in reference.conf
        }
        ConfigFactory.invalidateCaches()
        val settings = Settings(ConfigFactory.load())
        settingsMap.put(root, settings)
        settings
      }
    )

  }

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
        val settings = getUpdatedSettings(root, metadata, env, gcpProject)
        MainServer.mapper.writeValueAsString(Services.domains()(settings))
      case "jobs" =>
        val settings = getUpdatedSettings(root, metadata, env, gcpProject)
        MainServer.mapper.writeValueAsString(Services.jobs()(settings))
      case "types" =>
        val settings = getUpdatedSettings(root, metadata, env, gcpProject)
        MainServer.mapper.writeValueAsString(Services.types()(settings))
      case _ =>
        val settings = getUpdatedSettings(root, metadata, env, gcpProject)
        core.run(args)(settings)
        MainServer.mapper.writeValueAsString(
          Response(settings.comet.rootServe.getOrElse("Should never happen"))
        )
    }
    response
  }
}

case class Response(serve: String)
