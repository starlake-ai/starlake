package com.ebiznext.comet.controllers

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object CometServer extends App with CometRoutes {
  implicit val system: ActorSystem = ActorSystem("CometServer")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  lazy val routes: Route = cometRoutes
  Http().bindAndHandle(routes, "localhost", 8080)

  println(s"Server online at http://localhost:8080/")

  Await.result(system.whenTerminated, Duration.Inf)
}
