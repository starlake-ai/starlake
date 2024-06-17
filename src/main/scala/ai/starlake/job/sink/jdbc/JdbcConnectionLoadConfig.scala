package ai.starlake.job.sink.jdbc

import ai.starlake.schema.model.{RowLevelSecurity, WriteStrategy, WriteStrategyType}
import ai.starlake.utils.Utils
import org.apache.spark.sql.DataFrame

case class JdbcConnectionLoadConfig(
  sourceFile: Either[String, DataFrame] = Left(""),
  outputDomainAndTableName: String = "",
  strategy: WriteStrategy = WriteStrategy(Some(WriteStrategyType.APPEND)),
  format: String = "jdbc",
  options: Map[String, String] = Map.empty,
  rls: Option[List[RowLevelSecurity]] = None,
  createTableIfAbsent: Boolean = false
) {
  override def toString: String = {
    val redactedOptions = Utils.redact(options)
    s"""JdbcConnectionLoadConfig(
       |  sourceFile: $sourceFile,
       |  outputDomainAndTableName: $outputDomainAndTableName,
       |  strategy: $strategy,
       |  format: $format,
       |  options: $redactedOptions,
       |  rls: $rls
       |)""".stripMargin
  }
}
