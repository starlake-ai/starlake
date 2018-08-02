package com.ebiznext.comet.controllers

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.ebiznext.comet.config.Settings

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object CometServer extends App with CometRoutes {
  implicit val system: ActorSystem = ActorSystem("CometServer")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  lazy val routes: Route = cometRoutes
  Http().bindAndHandle(routes, Settings.interface, Settings.port)

  println(s"Server online at http://${Settings.interface}:${Settings.port}/")

  Await.result(system.whenTerminated, Duration.Inf)
}
