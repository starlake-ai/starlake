package ai.starlake.job.sink.bigquery

import ai.starlake.schema.model.{AccessControlEntry, Engine, RowLevelSecurity, Schema}
import ai.starlake.utils.CliConfig
import org.apache.spark.sql.DataFrame
import scopt.OParser

case class BigQueryLoadConfig(
  source: Either[String, DataFrame] = Left(""),
  outputDataset: String = "",
  outputTable: String = "",
  outputPartition: Option[String] = None,
  outputClustering: Seq[String] = Nil,
  sourceFormat: String = "",
  createDisposition: String = "",
  writeDisposition: String = "",
  location: Option[String] = None,
  days: Option[Int] = None,
  rls: Option[List[RowLevelSecurity]] = None,
  requirePartitionFilter: Boolean = false,
  engine: Engine = Engine.SPARK,
  options: Map[String, String] = Map.empty,
  partitionsToUpdate: Option[List[String]] = None,
  acl: Option[List[AccessControlEntry]] = None,
  starlakeSchema: Option[Schema] = None,
  domainTags: Option[Set[String]] = None
) {
  def getLocation(): String = this.location.getOrElse("EU")
}

object BigQueryLoadConfig extends CliConfig[BigQueryLoadConfig] {
  val command = "bqload"
  val parser: OParser[Unit, BigQueryLoadConfig] = {
    val builder = OParser.builder[BigQueryLoadConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("source_file")
        .action((x, c) => c.copy(source = Left(x)))
        .text("Full Path to source file")
        .required(),
      opt[String]("output_dataset")
        .action((x, c) => c.copy(outputDataset = x))
        .text("BigQuery Output Dataset")
        .required(),
      opt[String]("output_table")
        .action((x, c) => c.copy(outputTable = x))
        .text("BigQuery Output Table")
        .required(),
      opt[String]("output_partition")
        .action((x, c) => c.copy(outputPartition = Some(x)))
        .text("BigQuery Partition Field")
        .optional(),
      opt[Boolean]("require_partition_filter")
        .action((x, c) => c.copy(requirePartitionFilter = x))
        .text("Require Partition Filter")
        .optional(),
      opt[Seq[String]]("output_clustering")
        .valueName("col1,col2...")
        .action((x, c) => c.copy(outputClustering = x))
        .text("BigQuery Clustering Fields")
        .optional(),
      opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(options = x))
        .text("BigQuery Sink Options")
        .optional(),
      opt[String]("source_format")
        .action((x, c) => c.copy(sourceFormat = x))
        .text(
          "Source Format eq. parquet. This option is ignored, Only parquet source format is supported at this time"
        ),
      opt[String]("create_disposition")
        .action((x, c) => c.copy(createDisposition = x))
        .text(
          "Big Query Create disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition"
        ),
      opt[String]("write_disposition")
        .action((x, c) => c.copy(writeDisposition = x))
        .text(
          "Big Query Write disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition"
        ),
      opt[Seq[String]]("row_level_security")
        .action((x, c) => c.copy(rls = Some(x.map(RowLevelSecurity.parse).toList)))
        .text(
          "value is in the form name,filter,sa:sa@mail.com,user:user@mail.com,group:group@mail.com "
        )
    )
  }

  // comet bqload  --source_file xxx --output_dataset domain --output_table schema --source_format parquet --create_disposition  CREATE_IF_NEEDED --write_disposition WRITE_TRUNCATE
  def parse(args: Seq[String]): Option[BigQueryLoadConfig] =
    OParser.parse(parser, args, BigQueryLoadConfig())
}
