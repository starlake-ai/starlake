package ai.starlake.job.ingest.strategies

import ai.starlake.config.Settings.JdbcEngine
import ai.starlake.schema.model.{MergeOn, StrategyOptions, StrategyType}
import ai.starlake.sql.SQLUtils

class BigQueryStrategiesBuilder extends StrategiesBuilder {
  def buildSQLForStrategy(
    strategy: StrategyOptions,
    selectStatement: String,
    fullTableName: String,
    targetTableColumns: List[String],
    targetTableExists: Boolean,
    truncate: Boolean,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine
  ): String = {
    val result =
      strategy.`type` match {
        case StrategyType.APPEND | StrategyType.OVERWRITE =>
          val quote = jdbcEngine.quote
          val targetColumnsAsSelectString =
            SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)
          buildMainSql(
            selectStatement,
            strategy,
            materializedView,
            targetTableExists,
            truncate,
            fullTableName
          ).mkString(";\n")

        case StrategyType.UPSERT_BY_KEY =>
          buildSqlForMergeByKey(
            selectStatement,
            fullTableName,
            targetTableExists,
            targetTableColumns,
            strategy,
            jdbcEngine
          )
        case StrategyType.UPSERT_BY_KEY_AND_TIMESTAMP =>
          buildSqlForMergeByKeyAndTimestamp(
            selectStatement,
            fullTableName,
            targetTableExists,
            targetTableColumns,
            strategy,
            jdbcEngine
          )
        case StrategyType.SCD2 =>
          buildSqlForSC2(
            selectStatement,
            fullTableName,
            targetTableExists,
            targetTableColumns,
            strategy,
            jdbcEngine
          )
        case StrategyType.OVERWRITE_BY_PARTITION =>
          buildSqlForPartitionOverwrite(
            selectStatement,
            fullTableName,
            targetTableExists,
            targetTableColumns,
            strategy,
            jdbcEngine
          )

        case unknownStrategy =>
          throw new Exception(s"Unknown strategy $unknownStrategy")
      }

    result
  }

  protected def buildSqlForMergeByKey(
    selectStatement: String,
    targetTableFullName: String,
    targetTableExists: Boolean,
    targetTableColumns: List[String],
    strategy: StrategyOptions,
    jdbcEngine: JdbcEngine
  ): String = {
    val mergeOn = strategy.on.getOrElse(MergeOn.SOURCE_AND_TARGET)
    val quote = jdbcEngine.quote
    val viewPrefix = jdbcEngine.viewPrefix

    val targetColumnsAsSelectString =
      SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)

    val mergeKeys =
      strategy.key
        .map(key => s"$quote$key$quote")
        .mkString(",")

    (targetTableExists, mergeOn) match {
      case (false, MergeOn.TARGET) =>
        /*
            The table does not exist, we can just insert the data
         */
        s"""WITH SL_INCOMING AS ($selectStatement)
           |SELECT  $targetColumnsAsSelectString
           |FROM SL_INCOMING
            """.stripMargin
      case (false, MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table does not exist, but we are asked to deduplicate the data from teh input
            We create a temporary table with a row number and select the first row for each partition
            And then we insert the data
         */
        s"""
           |WITH
           |SL_INCOMING AS ($selectStatement),
           |SL_VIEW_WITH_ROWNUM AS (
           |  SELECT  $targetColumnsAsSelectString,
           |          ROW_NUMBER() OVER (PARTITION BY $mergeKeys ORDER BY (select 0)) AS SL_SEQ
           |  FROM SL_INCOMING)
           |SELECT  $targetColumnsAsSelectString  FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1
            """.stripMargin
      case (true, MergeOn.TARGET) =>
        /*
            The table exists, we can merge the data
         */
        val paramsForInsertSql = {
          val targetColumns = SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)
          val sourceColumns =
            SQLUtils.incomingColumnsForSelectSql("SL_INCOMING", targetTableColumns, quote)
          s"""($targetColumns) VALUES ($sourceColumns)"""
        }
        val mergeKeyJoinCondition =
          strategy.key
            .map(key => s"SL_INCOMING.$quote$key$quote = SL_EXISTING.$quote$key$quote")
            .mkString(" AND ")

        val paramsForUpdateSql = SQLUtils.setForUpdateSql("SL_INCOMING", targetTableColumns, quote)

        s"""
           |MERGE INTO $targetTableFullName SL_EXISTING USING ($selectStatement) SL_INCOMING ON ($mergeKeyJoinCondition)
           |WHEN MATCHED THEN UPDATE $paramsForUpdateSql
           |WHEN NOT MATCHED THEN INSERT $paramsForInsertSql
           |""".stripMargin

      case (true, MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table exists, We deduplicated the data from the input and we merge the data
         */
        val paramsForInsertSql = {
          val targetColumns = SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)
          val sourceColumns =
            SQLUtils.incomingColumnsForSelectSql(
              s"SL_INCOMING",
              targetTableColumns,
              quote
            )
          s"""($targetColumns) VALUES ($sourceColumns)"""
        }
        val mergeKeyJoinCondition =
          strategy.key
            .map(key => s"SL_INCOMING.$quote$key$quote = SL_EXISTING.$quote$key$quote")
            .mkString(" AND ")

        val paramsForUpdateSql =
          SQLUtils.setForUpdateSql(s"SL_INCOMING", targetTableColumns, quote)

        val slDedup =
          s"""
             |SELECT  $targetColumnsAsSelectString
             |FROM (
             |  SELECT $targetColumnsAsSelectString, ROW_NUMBER() OVER (PARTITION BY $mergeKeys  ORDER BY (select 0)) AS SL_SEQ
             |  FROM ($selectStatement)
             |)
             |WHERE SL_SEQ = 1
             |""".stripMargin
        s"""
           |MERGE INTO $targetTableFullName SL_EXISTING USING ($slDedup) SL_INCOMING ON ($mergeKeyJoinCondition)
           |WHEN MATCHED THEN UPDATE $paramsForUpdateSql
           |WHEN NOT MATCHED THEN INSERT $paramsForInsertSql
           |""".stripMargin
      case (_, MergeOn(_)) =>
        throw new Exception("Should never happen !!!")

    }
  }
  private def buildSqlForMergeByKeyAndTimestamp(
    selectStatement: String,
    targetTableFullName: String,
    targetTableExists: Boolean,
    targetTableColumns: List[String],
    strategy: StrategyOptions,
    jdbcEngine: JdbcEngine
  ): String = {
    val mergeTimestampCol = strategy.timestamp
    val mergeOn = strategy.on.getOrElse(MergeOn.SOURCE_AND_TARGET)
    val quote = jdbcEngine.quote
    val canMerge = jdbcEngine.canMerge
    val viewPrefix = jdbcEngine.viewPrefix

    val targetColumnsAsSelectString =
      SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)

    val mergeKeys =
      strategy.key
        .map(key => s"$quote$key$quote")
        .mkString(",")

    (targetTableExists, mergeTimestampCol, mergeOn) match {
      case (false, Some(_), MergeOn.TARGET) =>
        /*
            The table does not exist, we can just insert the data
         */
        s"""WITH SL_INCOMING AS ($selectStatement)
           |SELECT  $targetColumnsAsSelectString  FROM SL_INCOMING""".stripMargin

      case (true, Some(mergeTimestampCol), MergeOn.TARGET) =>
        /*
            The table exists, we can merge the data by joining on the key and comparing the timestamp
         */
        val mergeKeyJoinCondition =
          SQLUtils.mergeKeyJoinCondition("SL_INCOMING", "SL_EXISTING", strategy.key, quote)

        val paramsForInsertSql = {
          val targetColumns = SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)
          val sourceColumns =
            SQLUtils.incomingColumnsForSelectSql("SL_INCOMING", targetTableColumns, quote)
          s"""($targetColumns) VALUES ($sourceColumns)"""
        }

        val paramsForUpdateSql =
          SQLUtils.setForUpdateSql("SL_INCOMING", targetTableColumns, quote)

        s"""
             |MERGE INTO $targetTableFullName SL_EXISTING USING ($selectStatement) SL_INCOMING ON ($mergeKeyJoinCondition)
             |WHEN MATCHED AND SL_INCOMING.$mergeTimestampCol > SL_EXISTING.$mergeTimestampCol THEN UPDATE $paramsForUpdateSql
             |WHEN NOT MATCHED THEN INSERT $paramsForInsertSql
             |""".stripMargin

      case (false, Some(mergeTimestampCol), MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table does not exist
            We create a temporary table with a row number and select the first row for each partition based on the timestamp
            And then we insert the data
         */
        s"""
           |WITH
           |SL_INCOMING AS ($selectStatement),
           |SL_VIEW_WITH_ROWNUM AS (
           |  SELECT  $targetColumnsAsSelectString,
           |          ROW_NUMBER() OVER (PARTITION BY $mergeKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
           |  FROM SL_INCOMING
           |  )
           |
           |SELECT  $targetColumnsAsSelectString FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1
            """.stripMargin

      case (true, Some(mergeTimestampCol), MergeOn.SOURCE_AND_TARGET) =>
        val mergeKeyJoinCondition =
          SQLUtils.mergeKeyJoinCondition("SL_INCOMING", "SL_EXISTING", strategy.key, quote)

        val paramsForInsertSql = {
          val targetColumns = SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)
          val sourceColumns =
            SQLUtils.incomingColumnsForSelectSql("SL_INCOMING", targetTableColumns, quote)
          s"""($targetColumns) VALUES ($sourceColumns)"""
        }

        val paramsForUpdateSql =
          SQLUtils.setForUpdateSql("SL_INCOMING", targetTableColumns, quote)

        val slDedup = s"""
             |  SELECT  $targetColumnsAsSelectString
             |  FROM (
             |  SELECT  $targetColumnsAsSelectString, ROW_NUMBER() OVER (PARTITION BY $mergeKeys  ORDER BY (select 0)) AS SL_SEQ
             |  FROM ($selectStatement)
             |  ) WHERE SL_SEQ = 1
             |"""
        s"""
             |MERGE INTO $targetTableFullName SL_EXISTING USING ($slDedup) SL_INCOMING ON ($mergeKeyJoinCondition)
             |WHEN MATCHED AND SL_INCOMING.$mergeTimestampCol > SL_EXISTING.$mergeTimestampCol THEN UPDATE $paramsForUpdateSql
             |WHEN NOT MATCHED THEN INSERT $paramsForInsertSql
             |""".stripMargin

      case (_, Some(_), MergeOn(_)) | (_, None, MergeOn(_)) =>
        throw new Exception("Should never happen !!!")
    }
  }

  private def buildSqlForSC2(
    selectStatement: String,
    targetTableFullName: String,
    targetTableExists: Boolean,
    targetTableColumns: List[String],
    strategy: StrategyOptions,
    jdbcEngine: JdbcEngine
  ) = {
    val viewPrefix = jdbcEngine.viewPrefix

    val mergeTimestampCol = strategy.timestamp
    val mergeOn = strategy.on.getOrElse(MergeOn.SOURCE_AND_TARGET)
    val quote = jdbcEngine.quote
    val canMerge = jdbcEngine.canMerge
    val targetColumnsAsSelectString =
      SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)

    val startTsCol = strategy.start_ts.getOrElse(throw new Exception("SCD2 requires start_ts"))
    val endTsCol = strategy.end_ts.getOrElse(throw new Exception("SCD2 requires end_ts"))

    val mergeKeys =
      strategy.key
        .map(key => s"$quote$key$quote")
        .mkString(",")

    (targetTableExists, mergeTimestampCol, mergeOn) match {
      case (false, Some(_), MergeOn.TARGET) =>
        /*
            The table does not exist, we can just insert the data
         */
        s"""
           |WITH SL_INCOMING AS ($selectStatement)
           |SELECT  $targetColumnsAsSelectString  FROM SL_INCOMING
            """.stripMargin
      case (false, Some(mergeTimestampCol), MergeOn.SOURCE_AND_TARGET) =>
        /*
            The table does not exist
            We create a temporary table with a row number and select the first row for each partition based on the timestamp
            And then we insert the data
         */
        s"""
           |WITH SL_INCOMING AS ($selectStatement),
           |SL_VIEW_WITH_ROWNUM AS (
           |  SELECT  $targetColumnsAsSelectString,
           |          ROW_NUMBER() OVER (PARTITION BY $mergeKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
           |  FROM SL_INCOMING)
           |SELECT  $targetColumnsAsSelectString  FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1
            """.stripMargin

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
          SQLUtils.mergeKeyJoinCondition("SL_INCOMING", "SL_EXISTING", strategy.key, quote)

        val nullJoinCondition =
          strategy.key.map(key => s"SL_EXISTING.`$key` IS NULL").mkString(" AND ")

        val paramsForUpdateSql =
          SQLUtils.setForUpdateSql("SL_INCOMING", targetTableColumns, quote)

        val upodatedRecords =
          s"""
             |SELECT $targetColumnsAsSelectString FROM ($selectStatement) SL_INCOMING, $targetTableFullName SL_EXISTING
             |WHERE $mergeKeyJoinCondition AND SL_EXISTING.$endTsCol IS NULL AND SL_INCOMING.$mergeTimestampCol > SL_EXISTING.$mergeTimestampCol)
             |""".stripMargin
        s"""
           |INSERT INTO $targetTableFullName
           |SELECT $targetColumnsAsSelectString, NULL AS $startTsCol, NULL AS $endTsCol FROM ($selectStatement)
           |LEFT JOIN $targetTableFullName ON ($mergeKeyJoinCondition AND $targetTableFullName.$endTsCol IS NULL)
           |WHERE $nullJoinCondition;
           |
           |
           |MERGE INTO $targetTableFullName SL_EXISTING USING ($upodatedRecords) SL_INCOMING ON ($mergeKeyJoinCondition)
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
          SQLUtils.mergeKeyJoinCondition("SL_INCOMING", targetTableFullName, strategy.key, quote)

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
           |SELECT $targetColumnsAsSelectString, NULL AS $startTsCol, NULL AS $endTsCol FROM ($selectStatement)
           |LEFT JOIN $targetTableFullName ON ($mergeKeyJoinCondition AND $targetTableFullName.$endTsCol IS NULL)
           |WHERE $nullJoinCondition;
           |
           |WITH SL_INCOMING AS ($selectStatement),
           |SL_VIEW_WITH_ROWNUM AS (
           |  SELECT  $targetColumnsAsSelectString,
           |          ROW_NUMBER() OVER (PARTITION BY $mergeKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
           |  FROM SL_INCOMING),
           |SL_DEDUP AS (
           |SELECT  $targetTableColumns  FROM SL_VIEW_WITH_ROWNUM WHERE SL_SEQ = 1),
           |
           |SL_UPDATED_RECORDS AS (
           |  SELECT $targetColumnsAsSelectString FROM SL_DEDUP, $targetTableFullName
           |  WHERE $mergeKeyJoinCondition2 AND $targetTableFullName.$endTsCol IS NULL AND SL_DEDUP.$mergeTimestampCol > $targetTableFullName.$mergeTimestampCol
           |)
           |MERGE INTO $targetTableFullName USING SL_UPDATED_RECORDS ON ($mergeKeyJoinCondition3)
           |WHEN MATCHED THEN UPDATE $paramsForUpdateSql, $startTsCol = $mergeTimestampCol, $endTsCol = NULL
           |""".stripMargin

      case (_, Some(_), MergeOn(_)) =>
        throw new Exception("Should never happen !!!")
      case (_, None, _) =>
        throw new Exception("SCD2 is not supported without a merge timestamp column")

    }
  }
  private def buildSqlForPartitionOverwrite(
    sourceTable: String,
    targetTableFullName: String,
    targetTableExists: Boolean,
    targetTableColumns: List[String],
    strategy: StrategyOptions,
    jdbcEngine: JdbcEngine
  ): String = {
    val mergeTimestampCol = strategy.timestamp
    s"""
         |declare incoming_partitions array<date>;
         |
         |set (incoming_partitions) = (
         |  select as struct array_agg(distinct date($mergeTimestampCol))
         |  from $sourceTable
         |);
         |
         |merge into $targetTableFullName dest
         |using $sourceTable src
         |on false
         |when not matched by source and date($mergeTimestampCol) in unnest(incoming_partitions) then delete
         |when not matched then insert $targetTableColumns values $targetTableColumns
         |""".stripMargin
  }
}
