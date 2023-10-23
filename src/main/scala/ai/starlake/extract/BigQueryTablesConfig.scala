package ai.starlake.extract

import ai.starlake.schema.model.WriteMode
import ai.starlake.utils.CliConfig
import scopt.OParser

case class BigQueryTablesConfig(
  writeMode: Option[WriteMode] = None,
  connectionRef: Option[String] = None,
  tables: Map[String, List[String]] = Map.empty,
  persist: Boolean = true
)

object BigQueryTablesConfig extends CliConfig[BigQueryTablesConfig] {
  val command = "bq2yml or bq-info"

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
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("write_mode")
        .action((x, c) => c.copy(writeMode = Some(WriteMode.fromString(x))))
        .text(s"One of ${WriteMode.writes}")
        .optional(),
      opt[String]("connection")
        .action((x, c) => c.copy(connectionRef = Some(x)))
        .text(s"Connection to use")
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
    OParser.parse(parser, args, BigQueryTablesConfig())
}
