package ai.starlake.schema.generator

import better.files.File
import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
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

  private def relatedTables(): List[String] = {
    schemaHandler.domains.flatMap(_.relatedTables())
  }

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    Yml2GraphVizConfig.parse(args) match {
      case Some(config) =>
        val fkTables = relatedTables().map(_.toLowerCase).toSet
        val dots =
          schemaHandler.domains.map(_.asDot(config.includeAllAttributes.getOrElse(true), fkTables))
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
