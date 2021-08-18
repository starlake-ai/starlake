package com.ebiznext.comet.services.launch

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.services.MainRoutes
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Success}

object Application {

  lazy val config: Config = ConfigFactory.load()

  private def startHttpServer(routes: Route)(implicit system: ActorSystem[_]): Unit = {

    val Interface = config.getString("http.interface")
    val Port = config.getInt("http.port")

    import system.executionContext
    val futureBinding = Http().newServerAt(Interface, Port).bind(routes)
    futureBinding.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info("Server online at http://{}:{}/", address.getHostString, address.getPort)
      case Failure(ex) =>
        system.log.error("Failed to bind HTTP endpoint, terminating system", ex)
        system.terminate()
    }
  }

  def main(args: Array[String]): Unit = {
    val rootBehavior = Behaviors.setup[Nothing] { context =>
      implicit val settings: Settings = Settings(config)
      val mainRoutes = new MainRoutes()
      startHttpServer(mainRoutes.routes)(context.system)

      Behaviors.empty
    }
    val system = ActorSystem[Nothing](rootBehavior, "CometApplication")
  }
}
