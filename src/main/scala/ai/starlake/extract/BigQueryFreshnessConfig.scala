package ai.starlake.extract

import ai.starlake.config.GcpConnectionConfig
import ai.starlake.utils.CliConfig
import scopt.OParser

case class BigQueryFreshnessConfig(
  gcpProjectId: Option[String] = None,
  gcpSAJsonKey: Option[String] = None,
  location: Option[String] = None,
  tables: Map[String, List[String]] = Map.empty,
  jobs: Seq[String] = Seq.empty
) extends GcpConnectionConfig

object BigQueryFreshnessConfig extends CliConfig[BigQueryFreshnessConfig] {
  val command = "bq-freshness"
  val parser: OParser[Unit, BigQueryFreshnessConfig] = {
    val builder = OParser.builder[BigQueryFreshnessConfig]
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
      opt[String]("location")
        .action { (x, c) => c.copy(location = Some(x)) }
        .optional()
        .text("location"),
      opt[Seq[String]]("tables")
        .action { (x, c) =>
          val tables = x.map(_.split(".")).map(tab => tab(0) -> tab(1)).groupBy(_._1).map {
            case (k, v) => (k, v.map(_._2))
          }
          c.copy(tables = tables.mapValues(_.toList))
        }
        .optional()
        .text("List of datasetName.tableName1,datasetName.tableName2 ..."),
      opt[Seq[String]]("jobs")
        .action { (x, c) => c.copy(jobs = x) }
        .optional()
        .text("List of job names")
    )
  }

  def parse(args: Seq[String]): Option[BigQueryFreshnessConfig] =
    OParser.parse(parser, args, BigQueryFreshnessConfig())
}
