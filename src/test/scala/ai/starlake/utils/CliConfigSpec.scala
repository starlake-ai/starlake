package ai.starlake.utils

import ai.starlake.TestHelper
import ai.starlake.extract.{BigQueryTablesConfig, ExtractDataConfig, ExtractSchemaConfig}
import ai.starlake.job.convert.Parquet2CSVConfig
import ai.starlake.job.infer.InferSchemaConfig
import ai.starlake.job.ingest.{ImportConfig, IngestConfig, LoadConfig}
import ai.starlake.job.metrics.MetricsConfig
import ai.starlake.job.sink.es.ESLoadConfig
import ai.starlake.job.sink.jdbc.JdbcConnectionLoadConfig
import ai.starlake.job.sink.kafka.KafkaJobConfig
import ai.starlake.job.site.SiteConfig
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.generator._
import ai.starlake.schema.handlers.ValidateConfig
import ai.starlake.serve.MainServerConfig
import better.files.File

class CliConfigSpec extends TestHelper {
  new WithSettings() {
    "Generate Documentation" should "succeed" in {
      val configMap = Map[String, CliConfig[_]](
        "validate"           -> ValidateConfig,
        "import"             -> ImportConfig,
        "extract-schema"     -> ExtractSchemaConfig,
        "extract-data"       -> ExtractDataConfig,
        "infer-schema"       -> InferSchemaConfig,
        "load"               -> LoadConfig,
        "transform"          -> TransformConfig,
        "table-dependencies" -> TableDependenciesConfig,
        "acl-dependencies"   -> AclDependenciesConfig,
        "task-dependencies"  -> AutoTaskDependenciesConfig,
        "site"               -> SiteConfig,
        "esload"             -> ESLoadConfig,
        "ingest"             -> IngestConfig,
        "metrics"            -> MetricsConfig,
        "parquet2csv"        -> Parquet2CSVConfig,
        "cnxload"            -> JdbcConnectionLoadConfig,
        "yml2xls"            -> Yml2XlsConfig,
        "xls2yml"            -> Xls2YmlConfig,
        "kafkaload"          -> KafkaJobConfig,
        "infer-ddl"          -> Yml2DDLConfig,
        "bq-info"            -> BigQueryTablesConfig,
        "serve"              -> MainServerConfig // ,
        // "yml2dag"        -> Yml2DagConfigForMain
      )
      val orderedMap = configMap.toList.sortBy { case (command, config) =>
        command
      }.zipWithIndex

      val mdMap = orderedMap.map { case ((command, config), index) =>
        command -> config.markdown(index + 1)
      }
      val mdPath =
        (File(getClass.getResource("/")) / "../../../docs/docs/cli").pathAsString
      mdMap.foreach { case (k, v) =>
        reflect.io.File(s"$mdPath/$k.md").writeAll(v)
      }
    }
  }
}
