package ai.starlake.job.strategies

import ai.starlake.config.Settings
import ai.starlake.config.Settings.JdbcEngine
import ai.starlake.job.strategies.StrategiesBuilder.TableComponents
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Utils
import better.files.Resource
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}
//import scala.reflect.runtime.{universe => ru}

class StrategiesBuilder extends StrictLogging {

  def buildSqlWithJ2(
    strategy: WriteStrategy,
    selectStatement: String,
    tableComponents: TableComponents,
    targetTableExists: Boolean,
    truncate: Boolean,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink,
    action: String
  )(implicit settings: Settings): String = {
    val context = StrategiesBuilder.StrategiesGenerationContext(
      strategy,
      selectStatement,
      tableComponents,
      targetTableExists,
      truncate,
      materializedView,
      jdbcEngine,
      sinkConfig
    )
    val paramMap = context.asMap().asInstanceOf[Map[String, Object]]
    Resource.asString(
      s"templates/write-strategies/${jdbcEngine.strategyBuilder.toLowerCase()}/${action.toLowerCase()}.j2"
    ) match {
      case Some(content) =>
        val jinjaOutput = Utils.parseJinjaTpl(content, paramMap)
        logger.info(jinjaOutput)
        jinjaOutput
      case None =>
        throw new RuntimeException(
          s"SQL Template not found in for ${jdbcEngine.strategyBuilder}/$action.j2"
        )
    }

  }

  def run(
    strategy: WriteStrategy,
    selectStatement: String,
    tableComponents: StrategiesBuilder.TableComponents,
    targetTableExists: Boolean,
    truncate: Boolean,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink
  )(implicit settings: Settings): String = {
    if (targetTableExists) {
      buildSqlWithJ2(
        strategy,
        selectStatement,
        tableComponents,
        targetTableExists,
        truncate,
        materializedView,
        jdbcEngine,
        sinkConfig,
        strategy.getEffectiveType().toString
      )
    } else {
      buildSqlWithJ2(
        strategy,
        selectStatement,
        tableComponents,
        targetTableExists,
        truncate,
        materializedView,
        jdbcEngine,
        sinkConfig,
        strategy.getEffectiveType().toString
      )
    }
  }

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
    // The last SQL may be a select. This what we are going to
    // transform into a create table as or merge into or update from / insert as
    val scd2StartTimestamp =
      strategy.startTs.getOrElse(throw new IllegalArgumentException("strategy requires startTs"))
    val scd2EndTimestamp =
      strategy.endTs.getOrElse(throw new IllegalArgumentException("strategy requires endTs"))
    val finalSqls =
      if (!tableExists) {
        // Table may not have been created yet
        // If table does not exist we know for sure that the sql request is a SELECT
        if (materializedView)
          List(s"CREATE MATERIALIZED VIEW $fullTableName AS $sqlWithParameters")
        else {
          if (strategy.getEffectiveType() == WriteStrategyType.SCD2) {
            val startTs =
              s"ALTER TABLE $fullTableName ADD COLUMN $scd2StartTimestamp TIMESTAMP"
            val endTs =
              s"ALTER TABLE $fullTableName ADD COLUMN $scd2EndTimestamp TIMESTAMP"
            List(
              s"CREATE TABLE $fullTableName AS ($sqlWithParameters)",
              startTs,
              endTs
            )
          } else
            List(
              s"CREATE TABLE $fullTableName AS ($sqlWithParameters)"
            )
        }
      } else {
        val mainSql = s"INSERT INTO $fullTableName $sqlWithParameters"
        val insertSqls =
          if (strategy.getEffectiveType() == WriteStrategyType.OVERWRITE) {
            // If we are in overwrite mode we need to drop the table/truncate before inserting
            if (materializedView) {
              List(
                s"DROP MATERIALIZED VIEW $fullTableName",
                s"CREATE MATERIALIZED VIEW $fullTableName AS $sqlWithParameters"
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
            dropSqls :+ mainSql
          }
        insertSqls
      }
    finalSqls
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
    val startTsCol = strategy.startTs.getOrElse(
      throw new Exception("SCD2 is not supported without a start timestamp column")
    )
    val endTsCol = strategy.endTs.getOrElse(
      throw new Exception("SCD2 is not supported without an end timestamp column")
    )
    val mergeTimestampCol = strategy.timestamp
    val mergeOn = strategy.on.getOrElse(MergeOn.SOURCE_AND_TARGET)
    val quote = jdbcEngine.quote
    val canMerge = jdbcEngine.canMerge
    val targetColumnsAsSelectString =
      SQLUtils.targetColumnsForSelectSql(targetTableColumns, quote)

    val incomingColumnsAsSelectString =
      SQLUtils.incomingColumnsForSelectSql(sourceTable, targetTableColumns, quote)

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

        val tempViewWithRowNum = tempViewName("SL_VIEW_WITH_ROWNUM")

        val mainSql = buildMainSql(
          s"""SELECT  $targetColumnsAsSelectString  FROM $tempViewWithRowNum WHERE SL_SEQ = 1"""",
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
           |SELECT $incomingColumnsAsSelectString, NULL AS $startTsCol, NULL AS $endTsCol FROM $sourceTable
           |LEFT JOIN $targetTableFullName ON ($mergeKeyJoinCondition AND $targetTableFullName.$endTsCol IS NULL)
           |WHERE $nullJoinCondition;
           |
           |CREATE TEMPORARY TABLE SL_UPDATED_RECORDS AS
           |SELECT $incomingColumnsAsSelectString FROM $sourceTable, $targetTableFullName
           |WHERE $mergeKeyJoinCondition AND $targetTableFullName.$endTsCol IS NULL AND $sourceTable.$mergeTimestampCol > $targetTableFullName.$mergeTimestampCol;
           |
           |MERGE INTO $targetTableFullName USING SL_UPDATED_RECORDS ON ($mergeKeyJoinCondition2)
           |WHEN MATCHED THEN UPDATE $paramsForUpdateSql, $startTsCol = $mergeTimestampCol, $endTsCol = NULL
           |WHEN NOT MATCHED THEN INSERT $paramsForInsertSql -- here just to make the SQL valid. Only the WHEN MATCHED is used
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
          SQLUtils.mergeKeyJoinCondition(
            tempViewName("SL_DEDUP"),
            targetTableFullName,
            strategy.key,
            quote
          )

        val mergeKeyJoinCondition3 =
          SQLUtils.mergeKeyJoinCondition(
            tempViewName("SL_UPDATED_RECORDS"),
            targetTableFullName,
            strategy.key,
            quote
          )

        val nullJoinCondition =
          strategy.key
            .map(key => s"$targetTableFullName.$quote$key$quote IS NULL")
            .mkString(" AND ")

        val paramsForUpdateSql =
          SQLUtils.setForUpdateSql(tempViewName("SL_UPDATED_RECORDS"), targetTableColumns, quote)

        val viewWithRowNumColumnsAsSelectString =
          SQLUtils.incomingColumnsForSelectSql(
            tempViewName("SL_VIEW_WITH_ROWNUM"),
            targetTableColumns,
            quote
          )

        val dedupColumnsAsSelectString =
          SQLUtils.incomingColumnsForSelectSql(tempViewName("SL_DEDUP"), targetTableColumns, quote)

        s"""
           |INSERT INTO $targetTableFullName
           |SELECT $incomingColumnsAsSelectString, NULL AS $startTsCol, NULL AS $endTsCol FROM $sourceTable
           |LEFT JOIN $targetTableFullName ON ($mergeKeyJoinCondition AND $targetTableFullName.$endTsCol IS NULL)
           |WHERE $nullJoinCondition;
           |
           |${createTemporaryView("SL_VIEW_WITH_ROWNUM")}  AS
           |  SELECT  $incomingColumnsAsSelectString,
           |          ROW_NUMBER() OVER (PARTITION BY $mergeKeys ORDER BY $quote$mergeTimestampCol$quote DESC) AS SL_SEQ
           |  FROM $sourceTable;
           |
           |${createTemporaryView("SL_DEDUP")}  AS
           |SELECT  $viewWithRowNumColumnsAsSelectString
           |  FROM ${tempViewName("SL_VIEW_WITH_ROWNUM")}
           |  WHERE SL_SEQ = 1;
           |
           |${createTemporaryView("SL_UPDATED_RECORDS")}  AS
           |SELECT $dedupColumnsAsSelectString
           |FROM ${tempViewName("SL_DEDUP")}, $targetTableFullName
           |WHERE $mergeKeyJoinCondition2
           |  AND $targetTableFullName.$endTsCol IS NULL
           |  AND ${tempViewName(
            "SL_DEDUP"
          )}.$mergeTimestampCol > $targetTableFullName.$mergeTimestampCol;
           |
           |MERGE INTO $targetTableFullName
           |USING ${tempViewName("SL_UPDATED_RECORDS")}
           |ON ($mergeKeyJoinCondition3)
           |WHEN MATCHED THEN UPDATE $paramsForUpdateSql, $startTsCol = SL_UPDATED_RECORDS.$quote$mergeTimestampCol$quote, $endTsCol = NULL
           |WHEN NOT MATCHED THEN INSERT $paramsForInsertSql -- here just to make the SQL valid. Only the WHEN MATCHED is used
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
    new StrategiesBuilder()
    /*
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol: ru.ClassSymbol =
      mirror.staticClass(className)
    val classMirror = mirror.reflectClass(classSymbol)

    val consMethodSymbol = classSymbol.primaryConstructor.asMethod
    val consMethodMirror = classMirror.reflectConstructor(consMethodSymbol)

    val strategyBuilder = consMethodMirror.apply().asInstanceOf[StrategiesBuilder]
    strategyBuilder
     */
  }

  implicit class JavaWriteStrategy(writeStrategy: WriteStrategy) {
    def asMap(jdbcEngine: JdbcEngine): Map[String, Any] = {
      Map(
        "strategyType"        -> writeStrategy.`type`.getOrElse(WriteStrategyType.APPEND).toString,
        "strategyTypes"       -> writeStrategy.types.getOrElse(Map.empty[String, String]).asJava,
        "strategyKey"         -> writeStrategy.key.asJava,
        "strategyTimestamp"   -> writeStrategy.timestamp.getOrElse(""),
        "strategyQueryFilter" -> writeStrategy.queryFilter.getOrElse(""),
        "strategyOn"          -> writeStrategy.on.getOrElse(MergeOn.TARGET).toString,
        "strategyStartTs"     -> writeStrategy.startTs.getOrElse(""),
        "strategyEndTs"       -> writeStrategy.endTs.getOrElse(""),
        "strategyKeyCsv"      -> writeStrategy.keyCsv(jdbcEngine.quote),
        "strategyKeyJoinCondition" -> writeStrategy.keyJoinCondition(
          jdbcEngine.quote,
          "SL_INCOMING",
          "SL_EXISTING"
        )
      )
    }
  }

  implicit class JavaJdbcEngine(jdbcEngine: JdbcEngine) {
    def asMap(): Map[String, Any] = {
      Map(
        "engineQuote"           -> jdbcEngine.quote,
        "engineCanMerge"        -> jdbcEngine.canMerge,
        "engineViewPrefix"      -> jdbcEngine.viewPrefix,
        "enginePreActions"      -> jdbcEngine.preActions,
        "engineStrategyBuilder" -> jdbcEngine.strategyBuilder
      )
    }
  }
  case class TableComponents(
    database: String,
    domain: String,
    name: String,
    columnNames: List[String]
  ) {
    def getFullTableName(): String = {
      (database, domain, name) match {
        case ("", "", _) => name
        case ("", _, _)  => s"$domain.$name"
        case (_, _, _)   => s"$database.$domain.$name"
      }
    }
    def paramsForInsertSql(quote: String): String = {
      val targetColumns = SQLUtils.targetColumnsForSelectSql(columnNames, quote)
      val tableIncomingColumnsCsv =
        SQLUtils.incomingColumnsForSelectSql("SL_INCOMING", columnNames, quote)
      s"""($targetColumns) VALUES ($tableIncomingColumnsCsv)"""
    }

    def asMap(jdbcEngine: JdbcEngine): Map[String, Any] = {
      val tableIncomingColumnsCsv =
        SQLUtils.incomingColumnsForSelectSql("SL_INCOMING", columnNames, jdbcEngine.quote)
      val tableInsert = "INSERT " + paramsForInsertSql(jdbcEngine.quote)
      val tableUpdate =
        "UPDATE " + SQLUtils.setForUpdateSql("SL_INCOMING", columnNames, jdbcEngine.quote)

      Map(
        "tableDatabase"           -> database,
        "tableDomain"             -> domain,
        "tableName"               -> name,
        "tableColumnNames"        -> columnNames.asJava,
        "tableFullName"           -> getFullTableName(),
        "tableParamsForInsertSql" -> paramsForInsertSql(jdbcEngine.quote),
        "tableParamsForUpdateSql" -> SQLUtils
          .setForUpdateSql("SL_INCOMING", columnNames, jdbcEngine.quote),
        "tableInsert" -> tableInsert,
        "tableUpdate" -> tableUpdate,
        "tableColumnsCsv" -> columnNames
          .map(col => s"${jdbcEngine.quote}$col${jdbcEngine.quote}")
          .mkString(","),
        "tableIncomingColumnsCsv" -> tableIncomingColumnsCsv
      )
    }
  }

  case class StrategiesGenerationContext(
    strategy: WriteStrategy,
    selectStatement: String,
    tableComponents: StrategiesBuilder.TableComponents,
    targetTableExists: Boolean,
    truncate: Boolean,
    materializedView: Boolean,
    jdbcEngine: JdbcEngine,
    sinkConfig: Sink
  ) {

    def asMap(): Map[String, Any] = {
      strategy.asMap(jdbcEngine) ++ tableComponents.asMap(jdbcEngine) ++ Map(
        "selectStatement"  -> selectStatement,
        "tableExists"      -> targetTableExists,
        "tableTruncate"    -> truncate,
        "materializedView" -> materializedView
      ) ++ jdbcEngine.asMap() ++ sinkConfig.toAllSinks().asMap()

    }
  }
}
