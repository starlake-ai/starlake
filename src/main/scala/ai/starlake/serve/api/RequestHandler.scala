package ai.starlake.serve.api

import ai.starlake.serve.MainServer
import ai.starlake.utils.Utils
import better.files.File

import java.io.IOException
import javax.servlet.ServletException
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

class RequestHandler extends HttpServlet {
  @throws[ServletException]
  @throws[IOException]
  override protected def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val params = req.getParameter("PARAMS").split(" ")
    val root = Option(req.getParameter("ROOT")).getOrElse(File.temp.pathAsString)
    val env = Option(req.getParameter("ENV"))
    val metadata = Option(req.getParameter("METADATA"))
    val gcpProject = Option(req.getParameter("GOOGLE_CLOUD_PROJECT"))
    System.out.println(s"PARAMS=${params.toList}")
    System.out.println(s"ROOT=$root")
    System.out.println(s"METADATA=$metadata")
    System.out.println(s"ENV=$env")
    System.out.println(s"GOOGLE_CLOUD_PROJECT=$gcpProject")
    try {
      val response = MainServer.run(root, metadata, params, env, gcpProject)
      resp.setStatus(HttpServletResponse.SC_OK)
      resp.getWriter.println(response)
      println(response)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        resp.getWriter.println(Utils.exceptionAsString(e))
    }
  }
}
