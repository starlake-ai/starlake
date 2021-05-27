package com.ebiznext.comet.schema.generator

import better.files.File
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.SchemaHandler
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

class Yml2GraphViz(schemaHandler: SchemaHandler) extends LazyLogging {

  val prefix = """
                     |digraph {
                     |graph [pad="0.5", nodesep="0.5", ranksep="2"];
                     |node [shape=plain]
                     |rankdir=LR;
                     |
                     |
                     |""".stripMargin

  val suffix = """
                     |}
                     |""".stripMargin

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    Yml2GraphVizConfig.parse(args) match {
      case Some(config) =>
        val dots = schemaHandler.domains.map(_.asDot(config.includeAllAttributes.getOrElse(true)))
        val result = prefix + dots.mkString("\n") + suffix
        config.output match {
          case None => println(result)
          case Some(output) =>
            val file = File(output)
            file.overwrite(result)
        }
      case _ =>
        println(Yml2GraphVizConfig.usage())
    }
  }
}
