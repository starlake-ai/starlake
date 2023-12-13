package ai.starlake.extract

import ai.starlake.job.Cmd
import ai.starlake.schema.model.WriteMode
import scopt.OParser

trait BigQueryTablesCmd extends Cmd[BigQueryTablesConfig] {

  private def buildTablesMap(tables: Seq[String]): Map[String, List[String]] = {
    tables
      .map { _.split('.') }
      .map { tab => if (tab.length == 1) tab(0) -> "*" else tab(0) -> tab(1) }
      .groupBy(_._1) // by domain
      .map { case (k, v) =>
        (k, v.map(_._2).toList)
      }

  }

  val parser: OParser[Unit, BigQueryTablesConfig] = {
    val builder = OParser.builder[BigQueryTablesConfig]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note(""),
      opt[String]("write")
        .action((x, c) => c.copy(writeMode = Some(WriteMode.fromString(x))))
        .text(s"One of ${WriteMode.writes}")
        .optional(),
      opt[String]("connection")
        .action((x, c) => c.copy(connectionRef = Some(x)))
        .text(s"Connection to use")
        .optional(),
      opt[String]("database")
        .action((x, c) => c.copy(database = Some(x)))
        .text(s"database / project id")
        .optional(),
      opt[Unit]("external")
        .action((_, c) => c.copy(external = true))
        .text(
          s"Include external datasets defined in _config.sl.yml instead of using other parameters of this command ? Defaults to false"
        )
        .optional(),
      opt[Seq[String]]("tables")
        .action { (x, c) =>
          c.copy(tables = buildTablesMap(x))
        }
        .optional()
        .text("List of datasetName.tableName1,datasetName.tableName2 ..."),
      opt[Boolean]("persist")
        .action { (x, c) => c.copy(persist = x) }
        .optional()
        .text("Persist results ?")
    )
  }

  def parse(args: Seq[String]): Option[BigQueryTablesConfig] =
    OParser.parse(parser, args, BigQueryTablesConfig(), setup)
}
