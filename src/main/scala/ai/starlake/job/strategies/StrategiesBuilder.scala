package ai.starlake.job.strategies

import ai.starlake.config.Settings
import ai.starlake.config.Settings.JdbcEngine
import ai.starlake.schema.model.{MergeOn, Sink, WriteStrategy, WriteStrategyType}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter
import com.typesafe.scalalogging.StrictLogging

import scala.reflect.runtime.{universe => ru}

trait StrategiesBuilder extends StrictLogging {
  def buildSQLForStrategy(
    strategy: WriteStrategy,
    selectStatement: String,
    fullTableName: String,
    targetTableColumns: List[String],
    targetTableExists: Boolean,
    truncate: Boolean,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink
  )(implicit settings: Settings): String

  protected def createTemporaryView(viewName: String): String = {
    s"CREATE OR REPLACE TEMPORARY VIEW $viewName"
  }

  protected def createTable(fullTableName: String, sparkSinkFormat: String): String = {
    s"CREATE TABLE $fullTableName"
  }

  protected def tempViewName(name: String) = name

  protected def buildMainSql(
    sqlWithParameters: String,
    strategy: WriteStrategy,
    materializedView: Boolean,
    tableExists: Boolean,
    truncate: Boolean,
    fullTableName: String,
    sinkConfig: Sink
  )(implicit settings: Settings): List[String] = {
    val allSqls = sqlWithParameters.splitSql()
    val preMainSqls = allSqls.dropRight(1)
    // The last SQL may be a select. This what wea re going to
    // transform into a create table as or merge into or update from / insert as
    val lastSql = allSqls.last
    val scd2StartTimestamp =
      strategy.start_ts.getOrElse(throw new IllegalArgumentException("strategy requires start_ts"))
    val scd2EndTimestamp =
      strategy.end_ts.getOrElse(throw new IllegalArgumentException("strategy requires end_ts"))
    val finalSqls =
      if (!tableExists) { // Table may have been created yet
        // If table does not exist we know for sure that the sql request is a SELECT
        if (materializedView)
          List(s"CREATE MATERIALIZED VIEW $fullTableName AS $lastSql")
        else {
          if (strategy.`type` == WriteStrategyType.SCD2) {
            val startTs =
              s"ALTER TABLE $fullTableName ADD COLUMN $scd2StartTimestamp TIMESTAMP"
            val endTs =
              s"ALTER TABLE $fullTableName ADD COLUMN $scd2EndTimestamp TIMESTAMP"
            List(
              s"CREATE TABLE $fullTableName AS ($lastSql)",
              startTs,
              endTs
            )
          } else
            List(
              s"CREATE TABLE $fullTableName AS ($lastSql)"
            )
        }
      } else {
        val columns = SQLUtils.extractColumnNames(lastSql).mkString(",")
        val mainSql = s"INSERT INTO $fullTableName($columns) $lastSql"
        val insertSqls =
          if (strategy.`type` == WriteStrategyType.OVERWRITE) {
            // If we are in overwrite mode we need to drop the table/truncate before inserting
            if (materializedView) {
              List(
                s"DROP MATERIALIZED VIEW $fullTableName",
                s"CREATE MATERIALIZED VIEW $fullTableName AS $lastSql"
              )
            } else {
              List(s"DELETE FROM $fullTableName WHERE TRUE", mainSql)
            }
          } else {
            val dropSqls =
              if (truncate)
                List(s"DELETE FROM $fullTableName WHERE TRUE")
              else
                Nil
            if (strategy.`type` == WriteStrategyType.SCD2) {}
            dropSqls :+ mainSql
          }
        insertSqls
      }
    preMainSqls ++ finalSqls
  }

  protected def buildSqlForSC2(
    sourceTable: String,
    targetTableFullName: String,
    targetTableExists: Boolean,
    targetTableColumns: List[String],
    strategy: WriteStrategy,
    truncate: Boolean,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink
  )(implicit settings: Settings): String = {
    val startTsCol = strategy.start_ts.getOrElse(
      throw new Exception("SCD2 is not supported without a start timestamp column")
    )
    val endTsCol = strategy.end_ts.getOrElse(
      throw new Exception("SCD2 is not supported without an end timestamp column")
    )
    val mergeTimestampCol = strategy.timestamp
    val mergeOn = strategy.on.getOrElse(MergeOn.SOURCE_AND_TARGET)
    val quote = jdbcEngine.quote
    val canMerge = jdbcEngine.canMerge
    val targetColumnsAsSelectString =
      SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)

    val paramsForInsertSql = {
      val targetColumns = SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)
      val sourceColumns =
        SQLUtils.incomingColumnsForSelectSql(sourceTable, targetTableColumns, quote)
      s"""($targetColumns) VALUES ($sourceColumns)"""
    }

    val mergeKeys =
      strategy.key
        .map(key => s"$quote$key$quote")
        .mkString(",")

    (targetTableExists, mergeTimestampCol, mergeOn) match {
      case (false, Some(_), MergeOn.TARGET) =>
        /*
            The table does not exist, we can just insert the data
         */
        buildMainSql(
          s"SELECT $targetColumnsAsSelectString FROM $sourceTable",
          strategy,
          materializedView,
          targetTableExists,
          truncate,
          targetTableFullName,
          sinkConfig
        ).mkString(";\n")

      case (false, Some(mergeTimestampCol), MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table does not exist
            We create a temporary table with a row number and select the first row for each partition based on the timestamp
            And then we insert the data
         */
        val mostRecentView = s"""
                                |${createTemporaryView("SL_VIEW_WITH_ROWNUM")} AS
                                |  SELECT  $targetColumnsAsSelectString,
                                |          ROW_NUMBER() OVER (PARTITION BY $mergeKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
                                |  FROM $sourceTable;
            """.stripMargin

        val tempViewName = tempViewName("SL_VIEW_WITH_ROWNUM")

        val mainSql = buildMainSql(
          s"""SELECT  $targetColumnsAsSelectString  FROM $tempViewName WHERE SL_SEQ = 1"""",
          strategy,
          materializedView,
          targetTableExists,
          truncate,
          targetTableFullName,
          sinkConfig
        )

        (mostRecentView :: mainSql).mkString(";\n")

      case (true, Some(mergeTimestampCol), MergeOn.TARGET) =>
        /*
            First we insert all new rows
            Then we create a temporary table with the updated rows
            Then we update the end_ts of the old rows
            Then we insert the new rows
                INSERT INTO $targetTable
                SELECT $allAttributesSQL, $mergeTimestampCol AS $startTsCol, NULL AS $endTsCol FROM $sourceTable AS $SL_INTERNAL_TABLE
                WHERE $key IN (SELECT DISTINCT $key FROM SL_UPDATED_RECORDS);
         */
        val mergeKeyJoinCondition =
          SQLUtils.mergeKeyJoinCondition(sourceTable, targetTableFullName, strategy.key, quote)

        val mergeKeyJoinCondition2 =
          SQLUtils.mergeKeyJoinCondition(
            "SL_UPDATED_RECORDS",
            targetTableFullName,
            strategy.key,
            quote
          )

        val nullJoinCondition =
          strategy.key
            .map(key => s"$targetTableFullName.$quote$key$quote IS NULL")
            .mkString(" AND ")

        val paramsForUpdateSql =
          SQLUtils.setForUpdateSql("SL_UPDATED_RECORDS", targetTableColumns, quote)

        s"""
           |INSERT INTO $targetTableFullName
           |SELECT $targetColumnsAsSelectString, NULL AS $startTsCol, NULL AS $endTsCol FROM $sourceTable
           |LEFT JOIN $targetTableFullName ON ($mergeKeyJoinCondition AND $targetTableFullName.$endTsCol IS NULL)
           |WHERE $nullJoinCondition;
           |
           |CREATE TEMPORARY TABLE SL_UPDATED_RECORDS AS
           |SELECT $targetColumnsAsSelectString FROM $sourceTable, $targetTableFullName
           |WHERE $mergeKeyJoinCondition AND $targetTableFullName.$endTsCol IS NULL AND $sourceTable.$mergeTimestampCol > $targetTableFullName.$mergeTimestampCol;
           |
           |MERGE INTO $targetTableFullName USING SL_UPDATED_RECORDS ON ($mergeKeyJoinCondition2)
           |WHEN MATCHED THEN UPDATE $paramsForUpdateSql, $startTsCol = $mergeTimestampCol, $endTsCol = NULL
           |""".stripMargin

      case (true, Some(mergeTimestampCol), MergeOn.SOURCE_AND_TARGET) =>
        /*
            First we insert all new rows
            Then we create a temporary table with the updated rows
            Then we update the end_ts of the old rows
            Then we insert the new rows
                INSERT INTO $targetTable
                SELECT $allAttributesSQL, $mergeTimestampCol AS $startTsCol, NULL AS $endTsCol FROM $sourceTable AS $SL_INTERNAL_TABLE
                WHERE $key IN (SELECT DISTINCT $key FROM SL_UPDATED_RECORDS);
         */
        val mergeKeyJoinCondition =
          SQLUtils.mergeKeyJoinCondition(sourceTable, targetTableFullName, strategy.key, quote)

        val mergeKeyJoinCondition2 =
          SQLUtils.mergeKeyJoinCondition("SL_DEDUP", targetTableFullName, strategy.key, quote)

        val mergeKeyJoinCondition3 =
          SQLUtils.mergeKeyJoinCondition(
            "SL_UPDATED_RECORDS",
            targetTableFullName,
            strategy.key,
            quote
          )

        val nullJoinCondition =
          strategy.key
            .map(key => s"$targetTableFullName.$quote$key$quote IS NULL")
            .mkString(" AND ")

        val paramsForUpdateSql =
          SQLUtils.setForUpdateSql("SL_UPDATED_RECORDS", targetTableColumns, quote)

        s"""
           |INSERT INTO $targetTableFullName
           |SELECT $targetColumnsAsSelectString, NULL AS $startTsCol, NULL AS $endTsCol FROM $sourceTable
           |LEFT JOIN $targetTableFullName ON ($mergeKeyJoinCondition AND $targetTableFullName.$endTsCol IS NULL)
           |WHERE $nullJoinCondition;
           |
           |${createTemporaryView("SL_VIEW_WITH_ROWNUM")}  AS
           |  SELECT  $targetColumnsAsSelectString,
           |          ROW_NUMBER() OVER (PARTITION BY $mergeKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
           |  FROM $sourceTable;
           |
           |${createTemporaryView("SL_DEDUP")}  AS
           |SELECT  $targetTableColumns
           |  FROM ${tempViewName("SL_VIEW_WITH_ROWNUM")}
           |  WHERE SL_SEQ = 1;
           |
           |CREATE TEMPORARY TABLE SL_UPDATED_RECORDS AS
           |SELECT $targetColumnsAsSelectString FROM SL_DEDUP, $targetTableFullName
           |WHERE $mergeKeyJoinCondition2 AND $targetTableFullName.$endTsCol IS NULL AND SL_DEDUP.$mergeTimestampCol > $targetTableFullName.$mergeTimestampCol;
           |
           |MERGE INTO $targetTableFullName USING SL_UPDATED_RECORDS ON ($mergeKeyJoinCondition3)
           |WHEN MATCHED THEN UPDATE $paramsForUpdateSql, $startTsCol = $mergeTimestampCol, $endTsCol = NULL
           |""".stripMargin
      case (_, Some(_), MergeOn(_)) =>
        throw new Exception("Should never happen !!!")
      case (_, None, _) =>
        throw new Exception("SCD2 is not supported without a merge timestamp column")

    }
  }

}

object StrategiesBuilder {
  def apply(className: String): StrategiesBuilder = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol: ru.ClassSymbol =
      mirror.staticClass(className)
    val consMethodSymbol = classSymbol.primaryConstructor.asMethod
    val classMirror = mirror.reflectClass(classSymbol)
    val consMethodMirror = classMirror.reflectConstructor(consMethodSymbol)
    val strategyBuilder = consMethodMirror.apply().asInstanceOf[StrategiesBuilder]
    strategyBuilder
  }
}
