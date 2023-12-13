package ai.starlake.job.sink.jdbc

import ai.starlake.schema.model.RowLevelSecurity
import ai.starlake.utils.Utils
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import org.apache.spark.sql.DataFrame

case class JdbcConnectionLoadConfig(
  sourceFile: Either[String, DataFrame] = Left(""),
  outputDomainAndTableName: String = "",
  createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED,
  writeDisposition: WriteDisposition = WriteDisposition.WRITE_APPEND,
  format: String = "jdbc",
  options: Map[String, String] = Map.empty,
  rls: Option[List[RowLevelSecurity]] = None
) {
  override def toString: String = {
    val redactedOptions = Utils.redact(options)
    s"""JdbcConnectionLoadConfig(
       |  sourceFile: $sourceFile,
       |  outputDomainAndTableName: $outputDomainAndTableName,
       |  createDisposition: $createDisposition,
       |  writeDisposition: $writeDisposition,
       |  format: $format,
       |  options: $redactedOptions,
       |  rls: $rls
       |)""".stripMargin
  }
}
