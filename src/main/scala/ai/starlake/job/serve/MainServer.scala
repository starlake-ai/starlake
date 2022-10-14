package ai.starlake.job.serve

import ai.starlake.config.Settings
import ai.starlake.job.Main
import better.files.File
import com.typesafe.config.ConfigFactory
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.servlet.ServletHandler

import java.io.IOException
import javax.servlet.ServletException
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

object MainServer {
  val settings: Settings = Settings(ConfigFactory.load())
  val rootServe: String = settings.comet.rootServe match {
    case None =>
      throw new Exception("root-serve should be defined to a folder")
    case Some(dir) =>
      File(dir).createDirectories()
      dir
  }

  def serve(config: MainServerConfig): Unit = {
    val server = new Server(config.port)
    val handler = new ServletHandler()
    server.setHandler(handler)
    handler.addServletWithMapping(classOf[RequestHandler], "/")
    server.start()
  }
}

class RequestHandler extends HttpServlet {
  @throws[ServletException]
  @throws[IOException]
  override protected def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val params = req.getParameter("PARAMS").split(" ")
    Main.main(params)
    resp.setStatus(HttpServletResponse.SC_OK)
    resp.getWriter.println(s"""{ "serve": "${MainServer.rootServe}"}""")
  }
}
