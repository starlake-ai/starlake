package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.job.ingest.IngestionJob
import ai.starlake.utils.{IngestionCounters, JsonSerializer, SparkUtils}
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
        val workflowStatements = this.secondStepSQL(List(tempTableName))

        val dropFirstStepTableSql = s"DROP TABLE IF EXISTS $tempTableName;"
        val loadTaskSQL = Map(
          "steps"           -> "2",
          "incomingDir"     -> incomingDir,
          "pattern"         -> pattern,
          "format"          -> format,
          "firstStep"       -> firstSTepCreateTableSqls,
          "addFileName"     -> List(addFileNameSql),
          "secondStep"      -> workflowStatements.task.asMap(),
          "dropFirstStep"   -> dropFirstStepTableSql,
          "tempTableName"   -> tempTableName,
          "targetTableName" -> targetTableName,
          "domain"          -> domain.finalName,
          "table"           -> starlakeSchema.finalName,
          "writeStrategy"   -> writeDisposition
        )
        workflowStatements
          .asMap()
          .updated(
            "task",
            JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(loadTaskSQL)
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
        val workflowStatements = this.secondStepSQL(List(targetTableName))
        val loadTaskSQL = Map(
          "steps"           -> "1",
          "incomingDir"     -> incomingDir,
          "pattern"         -> pattern,
          "format"          -> format,
          "createTable"     -> createTableSqls,
          "targetTableName" -> targetTableName,
          "domain"          -> domain.finalName,
          "table"           -> starlakeSchema.finalName,
          "writeStrategy"   -> writeDisposition
        )
        workflowStatements
          .asMap()
          .updated(
            "task",
            JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(loadTaskSQL)
          )
      }
    result
  }
}
