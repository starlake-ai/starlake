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
        "import"       -> ImportConfig.markdown(1),
        "bqload"       -> BigQueryLoadConfig.markdown(2),
        "esload"       -> ESLoadConfig.markdown(3),
        "infer-schema" -> InferSchemaConfig.markdown(4),
        "load"         -> LoadConfig.markdown(5),
        "metrics"      -> MetricsConfig.markdown(6),
        "parquet2csv"  -> Parquet2CSVConfig.markdown(7),
        "cnxload"      -> ConnectionLoadConfig.markdown(8),
        "xls2yml"      -> Xls2YmlConfig.markdown(9),
        "ddl2yml"      -> DDL2YmlConfig.markdown(10),
        "extract"      -> ExtractScriptGenConfig.markdown(11),
        "transform"    -> TransformConfig.markdown(12),
        "watch"        -> WatchConfig.markdown(13),
        "kafkaload"    -> KafkaJobConfig.markdown(14),
        "yml2xls"      -> Yml2XlsConfig.markdown(15)
      )

      val rstPath =
        getClass.getResource("/").getPath + "../../../docs/docs/cli"
      rstMap.foreach { case (k, v) =>
        reflect.io.File(s"$rstPath/$k.md").writeAll(v)
      }
    }
  }
}
