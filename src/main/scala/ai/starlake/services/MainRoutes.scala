package ai.starlake.services

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directives, Route}
import com.typesafe.scalalogging.StrictLogging

class MainRoutes(
  generatorService: GeneratorService
) extends Directives
    with StrictLogging {
  lazy val apiRoutes: Route = generatorService.route

  def routes: Route = {
    logRequestResult("RestAll") {
      pathPrefix("api" / "comet-service") {
        pathEnd {
          complete(StatusCodes.OK, "this is the main root")
        } ~ apiRoutes
      }
    }
  }

}
