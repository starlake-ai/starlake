package com.ebiznext.comet.services.launch

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.services.{GeneratorService, MainRoutes}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Application extends App {

  implicit val system: ActorSystem = ActorSystem()

  lazy val config: Config = ConfigFactory.load()
  implicit val settings: Settings = Settings(config)

  lazy val Interface = config.getString("http.interface")
  lazy val Port = config.getInt("http.port")

  val generatorService = new GeneratorService
  val mainRoutes = new MainRoutes(generatorService)

//  Http().newServerAt(Interface, Port).bindFlow(mainRoutes.routes)

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  Http().bindAndHandle(mainRoutes.routes, Interface, Port)

  println(s"Server online at http://${Interface}:${Port}/")

  Await.result(system.whenTerminated, Duration.Inf)

}
