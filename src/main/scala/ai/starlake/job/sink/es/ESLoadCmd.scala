package ai.starlake.job.sink.es

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import org.apache.hadoop.fs.Path
import scopt.OParser

import scala.util.Try

trait ESLoadCmd extends Cmd[ESLoadConfig] {

  val command = "esload"

  val parser: OParser[Unit, ESLoadConfig] = {
    val builder = OParser.builder[ESLoadConfig]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note(""),
      opt[String]("timestamp")
        .action((x, c) => c.copy(timestamp = Some(x)))
        .optional()
        .text("Elasticsearch index timestamp suffix as in {@timestamp|yyyy.MM.dd}"),
      opt[String]("id")
        .action((x, c) => c.copy(id = Some(x)))
        .optional()
        .text("Elasticsearch Document Id"),
      opt[String]("mapping")
        .action((x, c) => c.copy(mapping = Some(new Path(x))))
        .optional()
        .text("Path to Elasticsearch Mapping File"),
      opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .required()
        .text("Domain Name"),
      opt[String]("schema")
        .action((x, c) => c.copy(schema = x))
        .required()
        .text("Schema Name"),
      opt[String]("format")
        .action((x, c) => c.copy(format = x))
        .required()
        .text("Dataset input file : parquet, json or json-array"),
      opt[String]("dataset")
        .action((x, c) => c.copy(dataset = Some(Left(new Path(x)))))
        .optional()
        .text("Input dataset path"),
      opt[Map[String, String]]("conf")
        .action((x, c) => c.copy(options = x))
        .optional()
        .valueName("es.batch.size.entries=1000, es.batch.size.bytes=1mb...")
        .text(
          """esSpark configuration options. See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html""".stripMargin
        )
    )
  }

  def parse(args: Seq[String]): Option[ESLoadConfig] =
    OParser.parse(parser, args, ESLoadConfig(), setup)

  override def run(config: ESLoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).esLoad(config).map(_ => JobResult.empty)

}

object ESLoadCmd extends ESLoadCmd
