package com.ebiznext.comet.app

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.scheduling.annotation.EnableScheduling

object Application extends App {
  val app = new SpringApplication(classOf[Application])
  app.run(args: _*)
}

@SpringBootApplication
@EnableScheduling
class Application
