package com.ebiznext.comet.utils

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.extractor.ExtractScriptGenConfig
import com.ebiznext.comet.job.convert.Parquet2CSVConfig
import com.ebiznext.comet.job.index.bqload.BigQueryLoadConfig
import com.ebiznext.comet.job.index.connectionload.ConnectionLoadConfig
import com.ebiznext.comet.job.index.esload.ESLoadConfig
import com.ebiznext.comet.job.index.kafkaload.KafkaJobConfig
import com.ebiznext.comet.job.infer.InferSchemaConfig
import com.ebiznext.comet.job.ingest.LoadConfig
import com.ebiznext.comet.job.metrics.MetricsConfig
import com.ebiznext.comet.schema.generator.{DDL2YmlConfig, Xls2YmlConfig, Yml2XlsConfig}
import com.ebiznext.comet.workflow.{ImportConfig, TransformConfig, WatchConfig}

class CliConfigSpec extends TestHelper {
  new WithSettings() {
    "Generate Documentation" should "succeed" in {
      val rstMap = Map(
        "import"       -> ImportConfig.sphinx(1),
        "bqload"       -> BigQueryLoadConfig.sphinx(2),
        "esload"       -> ESLoadConfig.sphinx(3),
        "infer-schema" -> InferSchemaConfig.sphinx(4),
        "load"         -> LoadConfig.sphinx(5),
        "metrics"      -> MetricsConfig.sphinx(6),
        "parquet2csv"  -> Parquet2CSVConfig.sphinx(7),
        "cnxload"      -> ConnectionLoadConfig.sphinx(8),
        "xls2yml"      -> Xls2YmlConfig.sphinx(9),
        "ddl2yml"      -> DDL2YmlConfig.sphinx(10),
        "extract"      -> ExtractScriptGenConfig.sphinx(11),
        "transform"    -> TransformConfig.sphinx(12),
        "watch"        -> WatchConfig.sphinx(13),
        "kafkaload"    -> KafkaJobConfig.sphinx(14),
        "yml2xls"      -> Yml2XlsConfig.sphinx(15)
      )

      val rstPath =
        getClass.getResource("/").getPath + "../../../../comet-docs/docs/cli"
      rstMap.foreach { case (k, v) =>
        reflect.io.File(s"$rstPath/$k.md").writeAll(v)
      }
    }
  }
}
