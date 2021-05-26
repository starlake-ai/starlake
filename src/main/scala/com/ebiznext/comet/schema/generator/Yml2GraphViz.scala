package com.ebiznext.comet.schema.generator

import better.files.File
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.SchemaHandler
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

class Yml2GraphViz(schemaHandler: SchemaHandler) extends LazyLogging {

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    Yml2GraphVizConfig.parse(args) match {
      case Some(config) =>
        val dots = schemaHandler.domains.map(_.asDot(config.includeAllAttributes.getOrElse(true)))
        config.output match {
          case None => println(dots.mkString("\n"))
          case Some(output) =>
            val file = File(output)
            file.overwrite(dots.mkString("\n"))
        }
      case _ =>
        println(Yml2XlsConfig.usage())
    }
  }
}
