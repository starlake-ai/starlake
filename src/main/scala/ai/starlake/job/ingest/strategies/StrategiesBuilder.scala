package ai.starlake.job.ingest.strategies

import ai.starlake.config.Settings
import ai.starlake.config.Settings.JdbcEngine
import ai.starlake.schema.model.{Sink, StrategyOptions, StrategyType}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter
import com.typesafe.scalalogging.StrictLogging

import scala.reflect.runtime.{universe => ru}

trait StrategiesBuilder extends StrictLogging {
  def buildSQLForStrategy(
    strategy: StrategyOptions,
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
    strategy: StrategyOptions,
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
          if (strategy.`type` == StrategyType.SCD2) {
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
          if (strategy.`type` == StrategyType.OVERWRITE) {
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
            if (strategy.`type` == StrategyType.SCD2) {}
            dropSqls :+ mainSql
          }
        insertSqls
      }
    preMainSqls ++ finalSqls
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
