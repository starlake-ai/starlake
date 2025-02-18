package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.job.ingest.IngestionJob
import ai.starlake.utils.{IngestionCounters, SparkUtils}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite

import scala.util.Try
class SnowflakeNativeLoader(ingestionJob: IngestionJob)(implicit
  settings: Settings
) extends NativeLoader(ingestionJob, None)
    with StrictLogging {

  def run(): Try[IngestionCounters] = ???

  def buildSQLStatements(): Map[String, Any] = {
    val twoSteps = this.twoSteps
    val targetTableName = s"${domain.finalName}.${starlakeSchema.finalName}"
    val tempTableName = s"${domain.finalName}.${this.tempTableName}"
    val incomingDir = domain.resolveDirectory()
    val pattern = starlakeSchema.pattern.toString
    val format = mergedMetadata.resolveFormat()

    val incomingSparkSchema = starlakeSchema.targetSparkSchemaWithIgnoreAndScript(schemaHandler)
    val ddlMap = schemaHandler.getDdlMapping(starlakeSchema)
    val options =
      new JdbcOptionsInWrite(sinkConnection.jdbcUrl, targetTableName, sinkConnection.options)

    val result =
      if (twoSteps) {
        val (tempCreateSchemaSql, tempCreateTableSql, _) = SparkUtils.buildCreateTableSQL(
          tempTableName,
          incomingSparkSchema,
          caseSensitive = false,
          options,
          ddlMap
        )
        val firstSTepCreateTableSqls = List(tempCreateSchemaSql, tempCreateTableSql)
        val addFileNameSql =
          s"ALTER TABLE $tempTableName ADD COLUMN ${CometColumns.cometInputFileNameColumn} STRING DEFAULT '{{sl_input_file_name}}';"
        val secondStepSQL = this.secondStepSQL(List(tempTableName))
        val dropFirstStepTableSql = s"DROP TABLE IF EXISTS $tempTableName;"
        Map(
          "twoSteps"      -> "yes",
          "incomingDir"   -> incomingDir,
          "pattern"       -> pattern,
          "format"        -> format,
          "firstStep"     -> firstSTepCreateTableSqls,
          "addFileName"   -> List(addFileNameSql),
          "secondStep"    -> secondStepSQL.asMap(),
          "dropFirstStep" -> dropFirstStepTableSql
        )
      } else {
        val (createSchemaSql, createTableSql, _) = SparkUtils.buildCreateTableSQL(
          targetTableName,
          incomingSparkSchema,
          caseSensitive = false,
          options,
          ddlMap
        )
        val createTableSqls = List(createSchemaSql, createTableSql)
        Map(
          "twoSteps"    -> "no",
          "incomingDir" -> incomingDir,
          "pattern"     -> pattern,
          "format"      -> format,
          "createTable" -> createTableSqls
        )
      }
    result
  }
}
