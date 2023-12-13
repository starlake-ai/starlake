package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

class TableDependencies(schemaHandler: SchemaHandler) extends LazyLogging {

  val prefix: String = """
                 |digraph {
                 |graph [pad="0.5", nodesep="0.5", ranksep="2"];
                 |node [shape=plain]
                 |rankdir=LR;
                 |
                 |
                 |""".stripMargin

  val suffix: String = """
                     |}
                     |""".stripMargin

  /** @param tableNames
    *   \- table names
    * @return
    *   (primary tables, fk tables)
    */
  private def relatedTablesForDot(tableNames: Seq[String]): (List[String], List[String]) = {
    // we extract all tables referenced by a foreign key in one of the tableNames parameter
    val foreignTableNames = schemaHandler.domains().flatMap(_.foreignTablesForDot(tableNames))

    val primaryTables = tableNames.flatMap { tableName =>
      schemaHandler
        .domains()
        .flatMap(d =>
          d.tables
            .filter(t => t.foreignTablesForDot(d.finalName).contains(tableName))
            .map(t => s"${d.finalName}.${t.finalName}")
        )
    }.distinct
    (primaryTables.toSet.toList, foreignTableNames.toSet.toList)
  }

  def run(args: Array[String]): Try[Unit] = {
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    TableDependenciesCmd.run(args, schemaHandler).map(_ => ())
  }

  def relationsAsDotFile(config: TableDependenciesConfig): Unit = {
    schemaHandler.domains(reload = config.reload)
    // we check if we have tables or domains
    val finalTables = config.tables match {
      case Some(tables) =>
        tables.flatMap { item =>
          if (item.contains('.')) {
            List(item) // it's already a table
          } else {
            // we have a domain, let's get all the tables
            schemaHandler.findTableNames(Some(item))
          }
        }.toList

      case None =>
        if (config.all) {
          schemaHandler.findTableNames(None)
        } else {
          Nil
        }
    }

//    val filteredTables = getTables(Some(finalTables))
    val (pkTables, sourceTables, fkTables) =
      if (config.related) {
        val (pkTables, fkTables) = relatedTablesForDot(finalTables)
        (pkTables.toSet, finalTables.toSet, fkTables.toSet)
      } else
        (Set.empty[String], finalTables.toSet, finalTables.toSet)

    val allTables = pkTables.union(sourceTables).union(fkTables)
    val dots =
      schemaHandler
        .domains()
        .map(_.asDot(config.includeAllAttributes, allTables.map(_.toLowerCase)))
    val dotStr = prefix + dots.mkString("\n") + suffix
    if (config.svg)
      Utils.dot2Svg(config.outputFile, dotStr)
    else if (config.png)
      Utils.dot2Png(config.outputFile, dotStr)
    else
      Utils.save(config.outputFile, dotStr)
  }
}
