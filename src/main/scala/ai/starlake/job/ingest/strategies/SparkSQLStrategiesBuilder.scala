package ai.starlake.job.ingest.strategies

import ai.starlake.config.Settings.JdbcEngine
import ai.starlake.schema.model.{MergeOn, StrategyOptions, StrategyType}
import ai.starlake.sql.SQLUtils

class SparkSQLStrategiesBuilder extends StrategiesBuilder {

  /** Assume incoming table is named SL_INCOMINH
    * @param strategy
    * @param fullTableName
    * @param allAttributes
    * @param defaultScd2StartTsCol
    * @param defaultScd2EndTsCol
    * @return
    */
  def buildSQLForStrategy(
    selectStatement: String,
    strategy: StrategyOptions,
    fullTableName: String,
    targetTableExists: Boolean,
    allAttributes: List[String],
    truncate: Boolean,
    materializedView: Boolean,
    engine: JdbcEngine
  ): String = {
    val updateAllAttributes = allAttributes.map(attr => s"$attr = SL_INCOMING.$attr").mkString(",")
    val selectAllAttributes = SQLUtils.targetColumnsForSelectSql(allAttributes, "")
    val insertAllAttributes = selectAllAttributes
    val preActions =
      s"""CREATE OR REPLACE TEMPORARY VIEW SL_INCOMING AS $selectStatement;
         |""".stripMargin
    val sqls =
      strategy.`type` match {
        case StrategyType.APPEND | StrategyType.OVERWRITE =>
          buildMainSql(
            s"""SELECT $selectAllAttributes FROM SL_INCOMING""",
            strategy,
            materializedView,
            targetTableExists,
            truncate,
            fullTableName
          ).mkString(";\n")

        case StrategyType.UPSERT_BY_KEY =>
          this.buildSqlForMergeByKey(
            "SL_INCOMING",
            fullTableName,
            targetTableExists,
            allAttributes,
            strategy,
            materializedView,
            engine
          )
        case StrategyType.UPSERT_BY_KEY_AND_TIMESTAMP =>
          buildSqlForMergeByKeyAndTimestamp(
            "SL_INCOMING",
            fullTableName,
            targetTableExists,
            allAttributes,
            strategy,
            materializedView,
            engine
          )
        case StrategyType.OVERWRITE_BY_PARTITION =>
          buildSqlForOverwriteByPartition(
            "SL_INCOMING",
            fullTableName,
            targetTableExists,
            allAttributes,
            strategy,
            materializedView,
            engine
          )

        case StrategyType.SCD2 =>
          val nullJoinCondition =
            strategy.key.map(key => s"$fullTableName.$key IS NULL").mkString(" AND ")
          val startTsCol =
            strategy.start_ts.getOrElse(throw new Exception("start_ts is required"))
          val endTsCol = strategy.end_ts.getOrElse(throw new Exception("end_ts is required"))
          ""
        case x => throw new Exception(s"Unsupported merge strategy $x")
      }
    preActions + sqls
  }

  private def buildSqlForOverwriteByPartition(
    sourceTable: String,
    targetTableFullName: String,
    targetTableExists: Boolean,
    targetTableColumns: List[String],
    strategy: StrategyOptions,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine
  ): String = {
    val quote = jdbcEngine.quote
    val targetColumnsAsSelectString =
      SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)

    val mergeTimestampCol =
      strategy.timestamp.getOrElse(throw new Exception("timestamp is required"))
    s"""/* merge into */
       |INSERT OVERWRITE TABLE $targetTableFullName PARTITION ($mergeTimestampCol) SELECT $targetColumnsAsSelectString FROM $sourceTable""".stripMargin
  }

  def buildSqlForMergeByKey(
    sourceTable: String,
    targetTableFullName: String,
    targetTableExists: Boolean,
    targetTableColumns: List[String],
    strategy: StrategyOptions,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine
  ): String = {
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
          fullTableName = targetTableFullName
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
          fullTableName = targetTableFullName
        ).mkString(";\n")
        s"""
           |${createTemporaryView("SL_VIEW_WITH_ROWNUM", jdbcEngine)} AS
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
    strategy: StrategyOptions,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine
  ): String = {
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
          fullTableName = targetTableFullName
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
          fullTableName = targetTableFullName
        ).mkString(";\n")

        s"""
           |${createTemporaryView("SL_VIEW_WITH_ROWNUM", jdbcEngine)} AS
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
