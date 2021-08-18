package com.ebiznext.comet.services

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directives, Route}
import com.ebiznext.comet.config.Settings
import com.typesafe.scalalogging.StrictLogging

class MainRoutes(implicit settings: Settings) extends Directives with StrictLogging {
  val generatorService = new GeneratorService
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
