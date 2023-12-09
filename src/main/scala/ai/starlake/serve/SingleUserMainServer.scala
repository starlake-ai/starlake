package ai.starlake.serve

import ai.starlake.utils.Utils
import buildinfo.BuildInfo
import com.fasterxml.jackson.databind.ObjectMapper
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.servlet.ServletHandler

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.net.InetSocketAddress
import scala.util.{Failure, Success, Try}

object SingleUserMainServer {
  val mapper: ObjectMapper = Utils.newJsonMapper()
  def serve(host: String, port: Int): Try[Unit] = Try {
    val server = new Server(new InetSocketAddress(host, port))
    val handler = new ServletHandler()
    server.setHandler(handler)
    handler.addServletWithMapping(classOf[SingleUserRequestHandler], "/")
    server.start()
    println(s"Server started at $host:$port")
    val o = new PrintStream(new File("/tmp/a.txt"))
    System.setOut(o)
    System.setErr(o)
  }

  private var autoReload = true
  def run(
    root: String,
    metadata: Option[String],
    args: Array[String],
    env: Option[String],
    gcpProject: Option[String]
  ): String = {
    val (settings, reload) =
      SettingsManager.getUpdatedSettings(root, metadata, env, gcpProject)
    if (args.head != "quit" && autoReload) {
      SingleUserMainServer.mapper.writeValueAsString(
        SingleUserServices.reset(reload = true)(settings)
      )
    }
    val response = args.head match {
      case "quit" | "exit" =>
        System.exit(0)
        "" // makes the compiler happy
      case "version" => SingleUserMainServer.mapper.writeValueAsString(BuildInfo.version)
      case "reload" =>
        if (args.last == "unset")
          autoReload = false
        else {
          if (args.last == "set") {
            autoReload = true
          }
          SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.reset(reload)(settings))
        }
      case "heartbeat" => SingleUserMainServer.mapper.writeValueAsString("OK")
      case "domains" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.domains(reload)(settings))
      case "table-names" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.objectNames()(settings))
      case "jobs" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.jobs(reload)(settings))
      case "types" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.types(reload)(settings))
      case _ =>
        val errCapture = new ByteArrayOutputStream()
        Console.withErr(errCapture) {
          val result = SingleUserServices.core(args, reload)(settings)
          result match {
            case Failure(e: IllegalArgumentException) =>
              s"""
              |--------------------------------------------------
              |${errCapture.toString().trim}
              |--------------------------------------------------
              |${e.getMessage}""".stripMargin
            case Failure(exception) =>
              val errMessage = Utils.exceptionAsString(exception)

              SingleUserMainServer.mapper.writeValueAsString(
                Response(errMessage)
              )
            case Success(_) =>
              SingleUserMainServer.mapper.writeValueAsString(
                Response(settings.appConfig.rootServe.getOrElse("Should never happen"))
              )
          }
        }
    }
    response
  }
}

case class Response(serve: String)
