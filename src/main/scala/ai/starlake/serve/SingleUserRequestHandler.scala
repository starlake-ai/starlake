package ai.starlake.serve

import ai.starlake.utils.Utils
import better.files.File

import java.io.IOException
import javax.servlet.ServletException
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

class SingleUserRequestHandler extends HttpServlet {
  @throws[ServletException]
  @throws[IOException]
  override protected def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val params = req.getParameter("PARAMS").split(" ")
    val root = Option(req.getParameter("ROOT")).getOrElse(File.temp.pathAsString)
    val env = Option(req.getParameter("ENV"))
    val metadata = Option(req.getParameter("METADATA"))
    val duckDbMode =
      Option(req.getParameter("SL_DUCKDB_MODE"))
        .orElse(Option(req.getParameter("SL_DUAL_MODE")))
        .getOrElse("false")
        .toBoolean
    val gcpProject =
      Option(req.getParameter("SL_DATABASE"))
        .filter(_.nonEmpty)
        .orElse(Option(req.getParameter("GCP_PROJECT")))
    System.out.println(s"PARAMS=${params.toList}")
    System.out.println(s"ROOT=$root")
    System.out.println(s"METADATA=$metadata")
    System.out.println(s"ENV=$env")
    System.out.println(s"SL_DATABASE=$gcpProject")
    System.out.println(s"SL_DUCKDB_MODE=$duckDbMode")
    System.out.println(s"SL_DUAL_MODE=$duckDbMode")
    try {
      val response = SingleUserMainServer.run(root, metadata, params, env, gcpProject, duckDbMode)
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
