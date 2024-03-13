package ai.starlake.job.strategies

import ai.starlake.config.Settings
import ai.starlake.config.Settings.JdbcEngine
import ai.starlake.schema.model.{FsSink, MergeOn, Sink, WriteStrategy, WriteStrategyType}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter

class SparkSQLStrategiesBuilder extends StrategiesBuilder {

  override protected def createTable(fullTableName: String, sparkSinkFormat: String): String = {
    s"CREATE TABLE $fullTableName USING $sparkSinkFormat"
  }

  override protected def buildMainSql(
    sqlWithParameters: String,
    strategy: WriteStrategy,
    materializedView: Boolean,
    tableExists: Boolean,
    truncate: Boolean,
    fullTableName: String,
    sinkConfig: Sink
  )(implicit settings: Settings): List[String] = {
    def sink = sinkConfig.asInstanceOf[FsSink]
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
      if (!tableExists) {
        // Table may have been created yet. Happen only for AutoTask, At ingestion, the table is always created upfront
        // If table does not exist we know for sure that the sql request is a SELECT
        if (materializedView)
          List(s"CREATE MATERIALIZED VIEW $fullTableName AS $lastSql")
        else {
          val mainSql =
            s"""CREATE TABLE $fullTableName
               |USING ${sink.getFormat()}
               |${sink.getTableOptionsClause()}
               |${sink.getPartitionByClauseSQL()}
               |${sink.getClusterByClauseSQL()}
               |AS ($lastSql)
               |""".stripMargin
          if (strategy.getStrategyType() == WriteStrategyType.SCD2) {
            val startTs =
              s"ALTER TABLE $fullTableName ADD COLUMN $scd2StartTimestamp TIMESTAMP"
            val endTs =
              s"ALTER TABLE $fullTableName ADD COLUMN $scd2EndTimestamp TIMESTAMP"

            List(
              mainSql,
              startTs,
              endTs
            )
          } else
            List(mainSql)
        }
      } else {
        val columns = SQLUtils.extractColumnNames(lastSql).mkString(",")
        val mainSql = s"INSERT INTO $fullTableName($columns) $lastSql"
        val insertSqls =
          if (strategy.getStrategyType() == WriteStrategyType.OVERWRITE) {
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
            if (strategy.getStrategyType() == WriteStrategyType.SCD2) {}
            dropSqls :+ mainSql
          }
        insertSqls
      }
    preMainSqls ++ finalSqls
  }

  /** Assume incoming table is named SL_INCOMINH
    * @param strategy
    * @param fullTableName
    * @param allAttributes
    * @param defaultScd2StartTsCol
    * @param defaultScd2EndTsCol
    * @return
    */

  def buildSQLForStrategy(
    strategy: WriteStrategy,
    selectStatement: String,
    fullTableName: String,
    targetTableColumns: List[String],
    targetTableExists: Boolean,
    truncate: Boolean,
    materializedView: Boolean,
    engine: JdbcEngine,
    sinkConfig: Sink
  )(implicit settings: Settings): String = {
    val updateAllAttributes =
      targetTableColumns.map(attr => s"$attr = SL_INCOMING.$attr").mkString(",")
    val selectAllAttributes = SQLUtils.targetColumnsForSelectSql(targetTableColumns, "")
    val insertAllAttributes = selectAllAttributes
    val preActions =
      s"""CREATE OR REPLACE TEMPORARY VIEW SL_INCOMING AS $selectStatement;
         |""".stripMargin
    val sqls =
      strategy.getStrategyType() match {
        case WriteStrategyType.APPEND | WriteStrategyType.OVERWRITE =>
          buildMainSql(
            s"""SELECT $selectAllAttributes FROM SL_INCOMING""",
            strategy,
            materializedView,
            targetTableExists,
            truncate,
            fullTableName,
            sinkConfig
          ).mkString(";\n")

        case WriteStrategyType.UPSERT_BY_KEY =>
          this.buildSqlForMergeByKey(
            "SL_INCOMING",
            fullTableName,
            targetTableExists,
            targetTableColumns,
            strategy,
            materializedView,
            engine,
            sinkConfig
          )
        case WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP =>
          buildSqlForMergeByKeyAndTimestamp(
            "SL_INCOMING",
            fullTableName,
            targetTableExists,
            targetTableColumns,
            strategy,
            materializedView,
            engine,
            sinkConfig
          )
        case WriteStrategyType.OVERWRITE_BY_PARTITION =>
          buildSqlForOverwriteByPartition(
            "SL_INCOMING",
            fullTableName,
            targetTableExists,
            targetTableColumns,
            strategy,
            materializedView,
            engine,
            sinkConfig
          )

        case WriteStrategyType.SCD2 =>
          buildSqlForSC2(
            "SL_INCOMING",
            fullTableName,
            targetTableExists,
            targetTableColumns,
            strategy,
            truncate,
            materializedView,
            engine,
            sinkConfig
          )
        case x => throw new Exception(s"Unsupported merge strategy $x")
      }
    preActions + sqls
  }

  private def buildSqlForOverwriteByPartition(
    sourceTable: String,
    targetTableFullName: String,
    targetTableExists: Boolean,
    targetTableColumns: List[String],
    strategy: WriteStrategy,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink
  )(implicit settings: Settings): String = {
    val quote = jdbcEngine.quote
    val targetColumnsAsSelectString =
      SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)

    val partitionColumns = sinkConfig
      .asInstanceOf[FsSink]
      .partition
      .getOrElse(throw new Exception("partition column is required"))
    val partitionColumnsAsString = partitionColumns.mkString(",")
    s"""INSERT OVERWRITE TABLE $targetTableFullName PARTITION ($partitionColumnsAsString) SELECT $targetColumnsAsSelectString FROM $sourceTable""".stripMargin
  }

  private def buildSqlForMergeByKey(
    sourceTable: String,
    targetTableFullName: String,
    targetTableExists: Boolean,
    targetTableColumns: List[String],
    strategy: WriteStrategy,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink
  )(implicit settings: Settings): String = {
    val viewPrefix = jdbcEngine.viewPrefix
    val quote = jdbcEngine.quote
    val mergeOn = strategy.on.getOrElse(MergeOn.SOURCE_AND_TARGET)

    val updateAllAttributes =
      targetTableColumns.map(attr => s"$attr = $sourceTable.$attr").mkString(",")
    val selectAllAttributes = SQLUtils.targetColumnsForSelectSql(targetTableColumns, "")
    val insertAllAttributes = selectAllAttributes
    val targetColumnsAsSelectString =
      SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)

    val mergeKeys =
      strategy.key
        .map(key => s"$quote$key$quote")
        .mkString(",")

    val joinCondition =
      strategy.key.map(k => s"$targetTableFullName.$k = $sourceTable.$k").mkString(" AND ")

    (targetTableExists, mergeOn) match {
      case (false, MergeOn.TARGET) =>
        /*
            The table does not exist, we can just insert the data
         */
        val sql =
          s"""SELECT  $targetColumnsAsSelectString
             |FROM $sourceTable
            """.stripMargin

        buildMainSql(
          sql,
          strategy,
          materializedView = false,
          targetTableExists,
          truncate = false,
          fullTableName = targetTableFullName,
          sinkConfig
        ).mkString(";\n")
      case (true, MergeOn.TARGET) =>
        s"""
           |MERGE INTO $targetTableFullName USING $sourceTable ON ($joinCondition)
           |WHEN MATCHED THEN UPDATE SET $updateAllAttributes
           |WHEN NOT MATCHED THEN INSERT ($insertAllAttributes) VALUES($insertAllAttributes)
           |""".stripMargin

      case (false, MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table does not exist, but we are asked to deduplicate the data from teh input
            We create a temporary table with a row number and select the first row for each partition
            And then we insert the data
         */
        val mainSql = buildMainSql(
          s"SELECT  $targetColumnsAsSelectString  FROM ${viewPrefix}SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1",
          strategy,
          materializedView = false,
          targetTableExists,
          truncate = false,
          fullTableName = targetTableFullName,
          sinkConfig
        ).mkString(";\n")
        s"""
           |${createTemporaryView("SL_VIEW_WITH_ROWNUM")} AS
           |  SELECT  $targetColumnsAsSelectString,
           |          ROW_NUMBER() OVER (PARTITION BY $mergeKeys ORDER BY (select 0)) AS SL_SEQ
           |  FROM $sourceTable;
           |$mainSql
            """.stripMargin

      case (true, MergeOn.SOURCE_AND_TARGET) =>
        /* We deduplicate the data from the input and we merge the data */
        val partitionKeys =
          strategy.key
            .map(key => s"$key")
            .mkString(",")
        s"""
           |CREATE OR REPLACE TEMPORARY VIEW SL_VIEW_WITH_ROWNUM AS
           |  SELECT  $targetColumnsAsSelectString,
           |          ROW_NUMBER() OVER (PARTITION BY $partitionKeys  ORDER BY (select 0)) AS SL_SEQ
           |  FROM $sourceTable;
           |CREATE OR REPLACE TEMPORARY VIEW SL_DEDUP AS SELECT  $targetColumnsAsSelectString FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1;
           |MERGE INTO $targetTableFullName USING SL_DEDUP AS SL_INCOMING ON ($joinCondition)
           |WHEN MATCHED THEN UPDATE SET $updateAllAttributes
           |WHEN NOT MATCHED THEN INSERT ($insertAllAttributes) VALUES($insertAllAttributes)
           |""".stripMargin
      case unknown =>
        throw new Exception(s"Unsupported merge on: $unknown")
    }
  }

  private def buildSqlForMergeByKeyAndTimestamp(
    sourceTable: String,
    targetTableFullName: String,
    targetTableExists: Boolean,
    targetTableColumns: List[String],
    strategy: WriteStrategy,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink
  )(implicit settings: Settings): String = {
    val viewPrefix = jdbcEngine.viewPrefix
    val quote = jdbcEngine.quote
    val mergeOn = strategy.on.getOrElse(MergeOn.SOURCE_AND_TARGET)

    val updateAllAttributes =
      targetTableColumns.map(attr => s"$attr = $sourceTable.$attr").mkString(",")
    val selectAllAttributes = SQLUtils.targetColumnsForSelectSql(targetTableColumns, "")
    val insertAllAttributes = selectAllAttributes
    val targetColumnsAsSelectString =
      SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)

    val mergeKeys =
      strategy.key
        .map(key => s"$quote$key$quote")
        .mkString(",")

    val joinCondition =
      strategy.key.map(k => s"$targetTableFullName.$k = $sourceTable.$k").mkString(" AND ")

    (targetTableExists, mergeOn) match {
      case (false, MergeOn.TARGET) =>
        /*
            The table does not exist, we can just insert the data
         */

        buildMainSql(
          s"""SELECT  $targetColumnsAsSelectString  FROM $sourceTable""",
          strategy,
          materializedView = false,
          targetTableExists,
          truncate = false,
          fullTableName = targetTableFullName,
          sinkConfig
        ).mkString(";\n")

      case (true, MergeOn.TARGET) =>
        val joinCondition =
          strategy.key
            .map(key => s"SL_INCOMING.$key = $targetTableFullName.$key")
            .mkString(" AND ")

        val mergeTimestampCol =
          strategy.timestamp.getOrElse(throw new Exception("timestamp is required"))
        s"""
           |MERGE INTO $targetTableFullName USING SL_INCOMING ON ($joinCondition)
           |WHEN MATCHED AND SL_INCOMING.$mergeTimestampCol > $targetTableFullName.$mergeTimestampCol THEN UPDATE SET $updateAllAttributes
           |WHEN NOT MATCHED THEN INSERT ($insertAllAttributes) VALUES($insertAllAttributes)
           |""".stripMargin

      case (false, MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table does not exist
            We create a temporary table with a row number and select the first row for each partition based on the timestamp
            And then we insert the data
         */
        val mergeTimestampCol =
          strategy.timestamp.getOrElse(throw new Exception("timestamp is required"))
        val mainSql = buildMainSql(
          s"""SELECT  $targetColumnsAsSelectString FROM ${viewPrefix}SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1""",
          strategy,
          materializedView = false,
          targetTableExists,
          truncate = false,
          fullTableName = targetTableFullName,
          sinkConfig
        ).mkString(";\n")

        s"""
           |${createTemporaryView("SL_VIEW_WITH_ROWNUM")} AS
           |  SELECT  $targetColumnsAsSelectString,
           |          ROW_NUMBER() OVER (PARTITION BY $mergeKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
           |  FROM $sourceTable;
           |
           |$mainSql
            """.stripMargin

      case (true, MergeOn.SOURCE_AND_TARGET) =>
        /* We deduplicated the data from the input and we merge the data */
        val mergeTimestampCol =
          strategy.timestamp.getOrElse(throw new Exception("timestamp is required"))
        s"""
           |CREATE OR REPLACE TEMPORARY VIEW SL_VIEW_WITH_ROWNUM AS
           |  SELECT  SL_INCOMING.*,
           |          ROW_NUMBER() OVER (PARTITION BY $mergeKeys  ORDER BY (select 0)) AS SL_SEQ
           |  FROM SL_INCOMING;
           |CREATE OR REPLACE TEMPORARY VIEW SL_DEDUP AS SELECT $selectAllAttributes  FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1;
           |MERGE INTO $targetTableFullName USING SL_DEDUP AS SL_INCOMING ON ($joinCondition)
           |WHEN MATCHED AND SL_INCOMING.$mergeTimestampCol > $targetTableFullName.$mergeTimestampCol THEN UPDATE SET $updateAllAttributes
           |WHEN NOT MATCHED THEN INSERT ($insertAllAttributes) VALUES($insertAllAttributes)
           |""".stripMargin

      case _ =>
        throw new Exception("Unsupported merge on")

    }
  }
}
