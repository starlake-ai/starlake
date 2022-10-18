package ai.starlake.job.serve

import ai.starlake.config.Settings
import ai.starlake.job.Main
import better.files.File
import buildinfo.BuildInfo
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.ConfigFactory
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.servlet.ServletHandler

import java.io.IOException
import javax.servlet.ServletException
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

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

  val main = new Main()

  def run(root: String, args: Array[String], env: Option[String]): String = {
    args.head match {
      case "quit" | "exit" =>
        System.exit(0)
        "" // makes the compiler happy
      case "version"   => BuildInfo.version
      case "heartbeat" => "" // do nothing
      case _ =>
        val settings = settingsMap.getOrElse(
          root, {
            settingsMap.clear() // For now we keep only one project in memory
            System.getProperties().setProperty("root", root)
            System.getProperties().setProperty("root-serve", File(root, "out").pathAsString)
            env match {
              case Some(env) if env.nonEmpty && env != "None" =>
                System.getProperties().setProperty("env", env)
              case _ =>
                System
                  .getProperties()
                  .setProperty("env", "prod") // prod is the default value in reference.conf
            }
            ConfigFactory.invalidateCaches()
            val settings = Settings(ConfigFactory.load())
            settingsMap.put(root, settings)
            settings
          }
        )
        main.run(args)(settings)
        settings.comet.rootServe.getOrElse("Should never happen")
    }
  }
}

case class Response(serve: String)

class RequestHandler extends HttpServlet {
  @throws[ServletException]
  @throws[IOException]
  override protected def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val params = req.getParameter("PARAMS").split(" ")
    val root = Option(req.getParameter("ROOT")).getOrElse(File.temp.pathAsString)
    val env = Option(req.getParameter("ENV"))
    System.out.println(s"PARAMS=${params.toList}")
    System.out.println(s"ROOT=$root")
    System.out.println(s"ENV=$env")
    val rootServe = MainServer.run(root, params, env)
    val response = Response(rootServe)
    resp.setStatus(HttpServletResponse.SC_OK)
    resp.getWriter.println(response)
  }
}
