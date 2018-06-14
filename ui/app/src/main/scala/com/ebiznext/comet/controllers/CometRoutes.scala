package com.ebiznext.comet.controllers

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.{get, post}
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.util.Timeout
import com.ebiznext.comet.utils.JsonSupport

import scala.concurrent.duration._

trait CometRoutes extends JsonSupport {
  implicit def system: ActorSystem

  lazy val log = Logging(system, classOf[CometRoutes])
  implicit lazy val timeout = Timeout(5.seconds) // usually we'd obtain the timeout from the system's configuration
  lazy val cometRoutes: Route =
    pathPrefix("nodes") {
      pathEnd {
        get {
          complete("")
        }
      }
    } ~
      pathPrefix("tags") {
        //#users-get-delete
        pathEnd {
          get {
            complete("")
          } ~
            post {
              complete("")
            }
        }
      } ~
      pathEnd {
        getFromResourceDirectory("webapp/index.html")
      } ~
      pathPrefix("webapp") {
        getFromResourceDirectory("webapp")
      }
}
