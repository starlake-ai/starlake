package com.ebiznext.comet.controllers

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.{get, post}
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.util.Timeout
import com.ebiznext.comet.utils.JsonSupport
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

trait CometRoutes extends JsonSupport {
  implicit def system: ActorSystem

  lazy val log = Logging(system, classOf[CometRoutes])
  implicit lazy val timeout = Timeout(5.seconds) // usually we'd obtain the timeout from the system's configuration
  val webapp = ConfigFactory.load().getString("webapp.root")

  lazy val cometRoutes: Route =
    pathPrefix("/api/nodes") {
      pathEnd {
        get {
          complete("")
        }
      }
    } ~
      pathPrefix("/api/tags") {
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
      pathSingleSlash {
        if (webapp.length > 0)
          getFromFile(s"$webapp/index.html")
        else
          getFromResource("$webapp/index.html")
      } ~
      path(Remaining) { remaining: String =>
        if (webapp.length > 0)
          getFromFile(s"$webapp/$remaining")
        else
          getFromResource(s"webapp/$remaining")


      }
}
