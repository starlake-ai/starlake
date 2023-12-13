package ai.starlake.utils

import ai.starlake.TestHelper
import ai.starlake.extract.{
  BigQueryFreshnessInfoCmd,
  BigQueryTableInfoCmd,
  ExtractCmd,
  ExtractDataCmd,
  ExtractJDBCSchemaCmd,
  ExtractScriptCmd
}
import ai.starlake.job.Cmd
import ai.starlake.job.bootstrap.BootstrapCmd
import ai.starlake.job.convert.Parquet2CSVCmd
import ai.starlake.job.infer.InferSchemaCmd
import ai.starlake.job.ingest.{IamPoliciesCmd, ImportCmd, IngestCmd, LoadCmd, SecureCmd}
import ai.starlake.job.metrics.MetricsCmd
import ai.starlake.job.sink.es.ESLoadCmd
import ai.starlake.job.sink.jdbc.JdbcConnectionLoadCmd
import ai.starlake.job.sink.kafka.KafkaJobCmd
import ai.starlake.job.site.SiteCmd
import ai.starlake.job.transform.TransformCmd
import ai.starlake.schema.ProjectCompareCmd
import ai.starlake.schema.generator._
import ai.starlake.schema.handlers.ValidateCmd
import ai.starlake.serve.MainServerCmd
import better.files.File

class CliConfigSpec extends TestHelper {
  new WithSettings() {
    "Generate Documentation" should "succeed" in {
      val commands: List[Cmd[_]] = List(
        BootstrapCmd,
        TransformCmd,
        ImportCmd,
        ValidateCmd,
        LoadCmd,
        IngestCmd,
        ESLoadCmd,
        KafkaJobCmd,
        JdbcConnectionLoadCmd,
        Yml2DDLCmd,
        InferSchemaCmd,
        MetricsCmd,
        Parquet2CSVCmd,
        SiteCmd,
        SecureCmd,
        IamPoliciesCmd,
        Xls2YmlCmd,
        Yml2XlsCmd,
        Xls2YmlAutoJobCmd,
        TableDependenciesCmd,
        AclDependenciesCmd,
        AutoTaskDependenciesCmd,
        ExtractCmd,
        ExtractJDBCSchemaCmd,
        ExtractDataCmd,
        ExtractScriptCmd,
        BigQueryTableInfoCmd,
        ExtractBigQuerySchemaCmd,
        BigQueryFreshnessInfoCmd,
        ProjectCompareCmd,
        MainServerCmd,
        DagGenerateCmd
      )
      val configMap: Map[String, CliConfig[_]] = commands.map { cmd =>
        cmd.command -> cmd
      }.toMap
      val orderedMap = configMap.toList.sortBy { case (command, _) =>
        command
      }.zipWithIndex

      val mdMap = orderedMap.map { case ((command, config), index) =>
        command -> config.markdown(index + 1)
      }
      val mdPath =
        (File(getClass.getResource("/")) / "../../../docs/docs/0800-cli").pathAsString
      mdMap.foreach { case (k, v) =>
        reflect.io.File(s"$mdPath/$k.md").writeAll(v)
      }
    }
  }
}
