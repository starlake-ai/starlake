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

  case class DomainItem(name: String, tables: List[String])
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
      case "version"   => SingleUserMainServer.mapper.writeValueAsString(BuildInfo.version)
      case "heartbeat" => SingleUserMainServer.mapper.writeValueAsString("OK")
      case "domains" =>
        SingleUserMainServer.mapper.writeValueAsString(Services.domains()(settings))
      case "table-names" =>
        val domains = Services.domains()(settings)
        val tableNames =
          domains.map { domain =>
            DomainItem(domain.finalName, domain.tables.map(_.finalName).sorted)
          }
        val jobs = Services.jobs()(settings)
        val taskNames =
          jobs.map { job =>
            DomainItem(job.name, job.tasks.map(_.table).sorted)
          }
        val all = tableNames ++ taskNames
        SingleUserMainServer.mapper.writeValueAsString(all.sortBy(_.name))
      case "jobs" =>
        SingleUserMainServer.mapper.writeValueAsString(Services.jobs()(settings))
      case "types" =>
        SingleUserMainServer.mapper.writeValueAsString(Services.types()(settings))
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
