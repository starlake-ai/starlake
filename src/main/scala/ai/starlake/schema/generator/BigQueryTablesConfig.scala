package ai.starlake.schema.generator

import ai.starlake.config.GcpConnectionConfig
import ai.starlake.schema.model.WriteMode
import ai.starlake.utils.CliConfig
import scopt.OParser

case class BigQueryTablesConfig(
  gcpProjectId: Option[String] = None,
  gcpSAJsonKey: Option[String] = None,
  writeMode: Option[WriteMode] = None,
  location: Option[String] = None,
  tables: Map[String, List[String]] = Map.empty
) extends GcpConnectionConfig

object BigQueryTablesConfig extends CliConfig[BigQueryTablesConfig] {
  val command = "bq2yml or b2-log"

  val parser: OParser[Unit, BigQueryTablesConfig] = {
    val builder = OParser.builder[BigQueryTablesConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("gcpProjectId")
        .action { (x, c) => c.copy(gcpProjectId = Some(x)) }
        .optional()
        .text("gcpProjectId"),
      opt[String]("gcpSAJsonKey")
        .action { (x, c) => c.copy(gcpSAJsonKey = Some(x)) }
        .optional()
        .text("gcpSAJsonKey"),
      opt[String]("write_mode")
        .action((x, c) => c.copy(writeMode = Some(WriteMode.fromString(x))))
        .text(s"One of ${WriteMode.writes}")
        .optional(),
      opt[String]("location")
        .action { (x, c) => c.copy(location = Some(x)) }
        .optional()
        .text("tables"),
      opt[Seq[String]]("tables")
        .action { (x, c) =>
          val tables = x.map(_.split(".")).map(tab => tab(0) -> tab(1)).groupBy(_._1).map {
            case (k, v) => (k, v.map(_._2))
          }
          c.copy(tables = tables.mapValues(_.toList))
        }
        .optional()
        .text("List of datasetName.tableName1,datasetName.tableName2 ...")
    )
  }

  def parse(args: Seq[String]): Option[BigQueryTablesConfig] =
    OParser.parse(parser, args, BigQueryTablesConfig())
}
