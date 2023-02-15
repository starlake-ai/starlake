package ai.starlake.utils

import ai.starlake.TestHelper
import ai.starlake.extract.{ExtractDataConfig, ExtractSchemaConfig, ExtractScriptConfig}
import ai.starlake.job.convert.Parquet2CSVConfig
import ai.starlake.job.infer.InferSchemaConfig
import ai.starlake.job.ingest.{ImportConfig, LoadConfig, WatchConfig}
import ai.starlake.job.metrics.MetricsConfig
import ai.starlake.job.sink.bigquery.BigQueryLoadConfig
import ai.starlake.job.sink.es.ESLoadConfig
import ai.starlake.job.sink.jdbc.ConnectionLoadConfig
import ai.starlake.job.sink.kafka.KafkaJobConfig
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.generator.{Xls2YmlConfig, Yml2GraphVizConfig, Yml2XlsConfig}
import better.files.File

class CliConfigSpec extends TestHelper {
  new WithSettings() {
    "Generate Documentation" should "succeed" in {
      val configMap = Map[String, CliConfig[_]](
        "import"         -> ImportConfig,
        "bqload"         -> BigQueryLoadConfig,
        "esload"         -> ESLoadConfig,
        "infer-schema"   -> InferSchemaConfig,
        "load"           -> LoadConfig,
        "metrics"        -> MetricsConfig,
        "parquet2csv"    -> Parquet2CSVConfig,
        "cnxload"        -> ConnectionLoadConfig,
        "xls2yml"        -> Xls2YmlConfig,
        "extract-schema" -> ExtractSchemaConfig,
        "extract-data"   -> ExtractDataConfig,
        "extract"        -> ExtractScriptConfig,
        "transform"      -> TransformConfig,
        "watch"          -> WatchConfig,
        "kafkaload"      -> KafkaJobConfig,
        "yml2xls"        -> Yml2XlsConfig,
        "yml2gv"         -> Yml2GraphVizConfig
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
