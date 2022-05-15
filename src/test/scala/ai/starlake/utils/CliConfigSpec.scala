package ai.starlake.utils

import ai.starlake.job.sink.bigquery.BigQueryLoadConfig
import ai.starlake.job.sink.jdbc.ConnectionLoadConfig
import ai.starlake.job.sink.es.ESLoadConfig
import ai.starlake.job.sink.kafka.KafkaJobConfig
import ai.starlake.schema.generator.{
  JDBC2YmlConfig,
  Xls2YmlConfig,
  Yml2GraphVizConfig,
  Yml2XlsConfig
}
import ai.starlake.TestHelper
import ai.starlake.extractor.ExtractScriptGenConfig
import ai.starlake.job.convert.Parquet2CSVConfig
import ai.starlake.job.sink.bigquery.BigQueryLoadConfig
import ai.starlake.job.sink.jdbc.ConnectionLoadConfig
import ai.starlake.job.sink.es.ESLoadConfig
import ai.starlake.job.sink.kafka.KafkaJobConfig
import ai.starlake.job.infer.InferSchemaConfig
import ai.starlake.job.ingest.LoadConfig
import ai.starlake.job.metrics.MetricsConfig
import ai.starlake.schema.generator.{
  JDBC2YmlConfig,
  Xls2YmlConfig,
  Yml2GraphVizConfig,
  Yml2XlsConfig
}
import ai.starlake.workflow.{ImportConfig, TransformConfig, WatchConfig}

class CliConfigSpec extends TestHelper {
  new WithSettings {
    "Generate Documentation" should "succeed" in {
      val rstMap = Map(
        "import"       -> ImportConfig.markdown(1),
        "bqload"       -> BigQueryLoadConfig.markdown(2),
        "esload"       -> ESLoadConfig.markdown(3),
        "infer-schema" -> InferSchemaConfig.markdown(4),
        "load"         -> LoadConfig.markdown(5),
        "metrics"      -> MetricsConfig.markdown(6),
        "parquet2csv"  -> Parquet2CSVConfig.markdown(7),
        "cnxload"      -> ConnectionLoadConfig.markdown(8),
        "xls2yml"      -> Xls2YmlConfig.markdown(9),
        "ddl2yml"      -> JDBC2YmlConfig.markdown(10),
        "extract"      -> ExtractScriptGenConfig.markdown(11),
        "transform"    -> TransformConfig.markdown(12),
        "watch"        -> WatchConfig.markdown(13),
        "kafkaload"    -> KafkaJobConfig.markdown(14),
        "yml2xls"      -> Yml2XlsConfig.markdown(15),
        "yml2gv"       -> Yml2GraphVizConfig.markdown(16)
      )

      val rstPath =
        getClass.getResource("/").getPath + "../../../docs/docs/cli"
      rstMap.foreach { case (k, v) =>
        reflect.io.File(s"$rstPath/$k.md").writeAll(v)
      }
    }
  }
}
