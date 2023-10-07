package ai.starlake.schema.generator

import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Utils
import better.files.File
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

class TableDependencies(schemaHandler: SchemaHandler) extends LazyLogging {

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

  private def relatedTables(tables: Option[Seq[String]]): List[String] = {
    schemaHandler.domains().flatMap(_.relatedTables(tables))
  }

  private def filterTables(tables: Option[Seq[String]]): List[String] = {
    schemaHandler.domains().flatMap(_.filterTables(tables).map(_.finalName))
  }

  def run(args: Array[String]): Try[Unit] = Try {
    TableDependenciesConfig.parse(args) match {
      case Some(config) =>
        relationsAsDotFile(config)
      case _ =>
    }
  }

  private def relationsAsDotFile(config: TableDependenciesConfig): Unit = {
    val result: String = relationsAsDotString(config)
    save(config, result)
  }

  private def save(config: TableDependenciesConfig, result: String): Unit = {
    config.outputFile match {
      case None => println(result)
      case Some(output) =>
        val outputFile = File(output)
        outputFile.parent.createDirectories()
        outputFile.overwrite(result)
    }
  }

  def relationsAsDotString(config: TableDependenciesConfig, svg: Boolean = false): String = {
    schemaHandler.domains(reload = config.reload)
    val fkTables =
      if (config.related)
        relatedTables(config.tables).toSet.union(filterTables(config.tables).toSet)
      else
        filterTables(config.tables)
    val dots =
      schemaHandler
        .domains()
        .map(_.asDot(config.includeAllAttributes, fkTables.map(_.toLowerCase).toSet))
    val dotStr = prefix + dots.mkString("\n") + suffix
    if (svg) {
      Utils.dot2Svg(dotStr)
    } else {
      dotStr
    }
  }
}
