package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.strategies.StrategiesBuilder
import ai.starlake.job.metrics.{ExpectationJob, JdbcExpectationAssertionHandler}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AccessControlEntry, AutoTaskDesc, StrategyType}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{JdbcJobResult, JobResult, SparkUtils, Utils}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.{Connection, Timestamp}
import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class JdbcAutoTask(
  taskDesc: AutoTaskDesc,
  commandParameters: Map[String, String],
  interactive: Option[String],
  truncate: Boolean,
  resultPageSize: Int = 1
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends AutoTask(
      taskDesc,
      commandParameters,
      interactive,
      truncate,
      resultPageSize
    ) {

  def extractJdbcAcl(): List[String] = {
    taskDesc.acl.flatMap { ace =>
      /*
        https://docs.snowflake.com/en/sql-reference/sql/grant-privilege
        https://hevodata.com/learn/snowflake-grant-role-to-user/
       */
      ace.asJdbcSql(fullTableName)
    }
  }

  def applyJdbcAcl(connection: Settings.Connection, forceApply: Boolean = false): Try[Unit] =
    AccessControlEntry.applyJdbcAcl(connection, extractJdbcAcl(), forceApply)

  override def run(): Try[JobResult] = {
    runJDBC(None)
  }

  override def tableExists: Boolean = {
    JdbcDbUtils.withJDBCConnection(sinkConnection.options) { conn =>
      val url = sinkConnection.options("url")
      val exists = JdbcDbUtils.tableExists(conn, url, fullTableName)
      if (!exists && taskDesc._auditTableName.isDefined)
        createAuditTable(
          conn
        ) // We are sinking to an audit table. We need to create it first in JDBC
      else
        exists
    }
  }

  def createAuditTable(conn: java.sql.Connection): Boolean = {
    // Table not found and it is an table in the audit schema defined in the reference-connections.conf file  Try to create it.
    logger.info(s"Table ${taskDesc.table} not found in ${taskDesc.domain}")
    val entry = taskDesc._auditTableName.getOrElse(
      throw new Exception(
        s"audit table for output ${taskDesc.table} is not defined in engine $jdbcSinkEngineName"
      )
    )
    val scriptTemplate = jdbcSinkEngine.tables(entry).createSql
    JdbcDbUtils.createSchema(conn, fullDomainName)

    val script = scriptTemplate.richFormat(
      Map("table" -> fullTableName, "writeFormat" -> settings.appConfig.defaultWriteFormat),
      Map.empty
    )
    JdbcDbUtils.executeUpdate(script, conn) match {
      case Success(_) =>
        true
      case Failure(e) =>
        logger.error(s"Error creating table $fullTableName", e)
        throw e
    }
  }

  def addSCD2Columns(connection: Connection): Unit = {
    this.taskDesc.strategy match {
      case Some(strategyOptions) if strategyOptions.`type` == StrategyType.SCD2 =>
        val startTsCol = strategyOptions.start_ts.getOrElse(settings.appConfig.scd2StartTimestamp)
        val endTsCol = strategyOptions.end_ts.getOrElse(settings.appConfig.scd2EndTimestamp)
        val scd2Columns = List(startTsCol, endTsCol)
        val alterTableSqls = scd2Columns.map { column =>
          s"ALTER TABLE $fullTableName ADD COLUMN IF NOT EXISTS $column TIMESTAMP"
        }
        runSqls(connection, alterTableSqls, "addSCE2Columns")
      case _ =>
    }
  }
  override def buildAllSQLQueries(sql: Option[String]): String = {
    assert(taskDesc.parseSQL.getOrElse(true))
    val sqlWithParameters = substituteRefTaskMainSQL(sql.getOrElse(taskDesc.getSql()))
    val columnNames = SQLUtils.extractColumnNames(sqlWithParameters)
    val mainSql = StrategiesBuilder(jdbcSinkEngine.strategyBuilder).buildSQLForStrategy(
      strategy,
      sqlWithParameters,
      fullTableName,
      columnNames,
      tableExists,
      truncate = truncate,
      materializedView = isMaterializedView(),
      jdbcSinkEngine,
      sinkConfig
    )
    mainSql
  }

  def runJDBC(df: Option[DataFrame]): Try[JdbcJobResult] = {
    val start = Timestamp.from(Instant.now())

    if (settings.appConfig.createSchemaIfNotExists) {
      // Creating a schema requires its own connection if called before a Spark save
      JdbcDbUtils.withJDBCConnection(sinkConnection.options) { conn =>
        JdbcDbUtils.createSchema(conn, fullDomainName)
      }
    }

    val res = Try {
      JdbcDbUtils.withJDBCConnection(sinkConnection.options) { conn =>
        val mainSql =
          if (df.isEmpty && taskDesc.parseSQL.getOrElse(true)) {
            buildAllSQLQueries(None)
          } else {
            taskDesc.getSql()
          }
        interactive match {
          case Some(_) =>
            runInteractive(conn, mainSql)
          case None =>
            conn.setAutoCommit(false)
            val parsedPreActions =
              Utils.parseJinja(jdbcSinkEngine.preactions, Map("schema" -> taskDesc.domain))
            Try {
              runPreActions(conn, parsedPreActions.splitSql(";"))
              runSqls(conn, preSql, "Pre")
              df match {
                case Some(loadedDF) =>
                  logger.info(s"Writing dataframe to $fullTableName")
                  val saveMode = strategy.`type`.toWriteMode().toSaveMode
                  if (saveMode == SaveMode.Overwrite || truncate) {
                    val jdbcUrl = sinkConnection.options("url")
                    val dialect = SparkUtils.dialect(jdbcUrl)
                    // We always append to the table to keep the schema (Spark loose the schema otherwise). We truncate using the truncate query option
                    JdbcDbUtils.withJDBCConnection(sinkConnection.options) { conn =>
                      SparkUtils.truncateTable(conn, fullTableName)
                    }
                  }
                  loadedDF.write
                    .format("jdbc")
                    .option("dbtable", fullTableName)
                    .mode(SaveMode.Append) // truncate done above if requested
                    .options(sinkConnection.options)
                    .save()
                case None =>
                  val finalSqls = mainSql.splitSql()
                  runSqls(conn, finalSqls, "Main")
              }

              applyJdbcAcl(sinkConnection, forceApply = true)
              runSqls(conn, postSql, "Post")
              addSCD2Columns(conn)

            } match {
              case Success(_) =>
                conn.commit()
              case Failure(e) =>
                conn.rollback()
                throw e
            }
            conn.setAutoCommit(true)

            if (settings.appConfig.expectations.active) {
              new ExpectationJob(
                taskDesc.database,
                taskDesc.domain,
                taskDesc.table,
                taskDesc.expectations,
                storageHandler,
                schemaHandler,
                new JdbcExpectationAssertionHandler(conn)
              ).run()
            }
            JdbcJobResult(Nil)
        }
      }
    }
    val end = Timestamp.from(Instant.now())
    res match {
      case Success(_) =>
        logAuditSuccess(start, end, -1)
      case Failure(e) =>
        logAuditFailure(start, end, e)
    }
    res
  }

  private def runInteractive(conn: Connection, mainSql: String): JdbcJobResult = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(mainSql)
      val result = new ListBuffer[List[String]]
      var i = 1
      val headerAsSeq = new ListBuffer[String]
      while (i <= rs.getMetaData.getColumnCount) {
        headerAsSeq.append(rs.getMetaData.getColumnName(i))
        i += 1
      }
      while (rs.next) {
        val rowAsSeq = new ListBuffer[String]
        var i = 1
        while (i <= rs.getMetaData.getColumnCount) {
          rowAsSeq.append(Option(rs.getObject(i)).map(_.toString).getOrElse(""))
          i += 1
        }
        result.append(rowAsSeq.toList)
      }
      JdbcJobResult(headerAsSeq.toList, result.toList)
    } finally {
      stmt.close()
    }
  }

  lazy val fullDomainName = taskDesc.database match {
    case Some(db) => s"$db.${taskDesc.domain}"
    case None     => taskDesc.domain
  }

  lazy val fullTableName = s"$fullDomainName.${taskDesc.table}"

  private def runPreActions(conn: java.sql.Connection, preactions: List[String]): Unit = {
    runSqls(conn, preactions, "Preactions")
  }

  @throws[Exception]
  private def runSqls(conn: Connection, sqls: List[String], typ: String): Unit = {
    if (sqls.nonEmpty) {
      logger.info(s"running $typ SQL")
      sqls.foreach { req =>
        JdbcDbUtils.executeUpdate(req, conn) match {
          case Success(_) =>
          case Failure(e) =>
            logger.error(s"Error running sql $req as $typ SQL", e)
            throw e
        }
      }
      logger.info(s"end running $typ SQL")
    }
  }

  def updateJdbcTableSchema(incomingSchema: StructType, tableName: String): Unit = {
    // update target table schema if needed
    val isSCD2 = strategy.`type` == StrategyType.SCD2
    val incomingSchemaWithSCD2 =
      if (isSCD2) {
        incomingSchema
          .add(
            StructField(
              strategy.start_ts
                .getOrElse(settings.appConfig.scd2StartTimestamp),
              TimestampType,
              nullable = true
            )
          )
          .add(
            StructField(
              strategy.end_ts.getOrElse(settings.appConfig.scd2EndTimestamp),
              TimestampType,
              nullable = true
            )
          )
      } else
        incomingSchema
    val sinkConnectionRefOptions = sinkConnection.options
    val jdbcUrl = sinkConnectionRefOptions("url")
    JdbcDbUtils.withJDBCConnection(sinkConnectionRefOptions) { conn =>
      val targetTableExists: Boolean = tableExists
      if (targetTableExists) {
        val existingSchema =
          SparkUtils.getSchemaOption(conn, sinkConnectionRefOptions, tableName)
        val addedSchema =
          SparkUtils.added(
            incomingSchemaWithSCD2,
            existingSchema.getOrElse(incomingSchema)
          )
        val deletedSchema =
          SparkUtils.dropped(
            incomingSchema,
            existingSchema.getOrElse(incomingSchema)
          )
        val alterTableDropColumns =
          SparkUtils.alterTableDropColumnsString(deletedSchema, tableName)
        if (alterTableDropColumns.nonEmpty) {
          logger.info(
            s"alter table $tableName with ${alterTableDropColumns.size} columns to drop"
          )
          logger.debug(s"alter table ${alterTableDropColumns.mkString("\n")}")
        }

        val alterTableAddColumns =
          SparkUtils.alterTableAddColumnsString(addedSchema, tableName)
        if (alterTableAddColumns.nonEmpty) {
          logger.info(
            s"alter table $tableName with ${alterTableAddColumns.size} columns to add"
          )
          logger.debug(s"alter table ${alterTableAddColumns.mkString("\n")}")
        }

        alterTableDropColumns.foreach(JdbcDbUtils.executeAlterTable(_, conn))
        alterTableAddColumns.foreach(JdbcDbUtils.executeAlterTable(_, conn))
        // At this point if the table exists, it has the same schema as the dataframe
        // And if it's a SCD2, it has the 2 extra timestamp columns
      } else {
        val optionsWrite =
          new JdbcOptionsInWrite(jdbcUrl, tableName, sinkConnectionRefOptions)
        logger.info(
          s"Table $tableName not found, creating it with schema $incomingSchemaWithSCD2"
        )
        SparkUtils.createTable(
          conn,
          tableName,
          incomingSchemaWithSCD2,
          caseSensitive = false,
          optionsWrite
        )
      }
    }
  }
}
