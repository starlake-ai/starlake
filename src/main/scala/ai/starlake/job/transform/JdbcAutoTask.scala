package ai.starlake.job.transform

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{ExtractSchemaCmd, ExtractSchemaConfig, JdbcDbUtils}
import ai.starlake.job.metrics.{ExpectationJob, JdbcExpectationAssertionHandler}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AccessControlEntry, AutoTaskDesc, Engine, WriteStrategyType}
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{JdbcJobResult, JobResult, SparkUtils, Utils}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.{Connection, Timestamp}
import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class JdbcAutoTask(
  appId: Option[String],
  taskDesc: AutoTaskDesc,
  commandParameters: Map[String, String],
  interactive: Option[String],
  truncate: Boolean,
  test: Boolean,
  logExecution: Boolean,
  accessToken: Option[String] = None,
  resultPageSize: Int = 1
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends AutoTask(
      appId,
      taskDesc,
      commandParameters,
      interactive,
      test,
      logExecution,
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

  def applyJdbcAcl(connection: Connection, forceApply: Boolean): Try[Unit] =
    AccessControlEntry.applyJdbcAcl(connection, extractJdbcAcl(), forceApply)

  def applyJdbcAcl(jdbcConnection: Settings.Connection, forceApply: Boolean): Try[Unit] =
    AccessControlEntry.applyJdbcAcl(jdbcConnection, extractJdbcAcl(), forceApply)

  override def run(): Try[JobResult] = {
    runJDBC(None)
  }

  override def tableExists: Boolean = {
    val exists =
      JdbcDbUtils.withJDBCConnection(
        JdbcDbUtils
          .readOnlyConnection(sinkConnection)(settings)
          .options
      ) { conn =>
        val url = sinkConnection.options("url")
        val exists = JdbcDbUtils.tableExists(conn, url, fullTableName)
        exists
      }
    if (!exists && taskDesc._auditTableName.isDefined)
      createAuditTable() // We are sinking to an audit table. We need to create it first in JDBC
    else
      exists
  }

  def createAuditTable(): Boolean = {
    // Table not found and it is an table in the audit schema defined in the reference-connections.conf file  Try to create it.
    JdbcDbUtils.withJDBCConnection(sinkConnection.options) { conn =>
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
      JdbcDbUtils.createSchema(conn, fullDomainName)
      JdbcDbUtils.executeUpdate(script, conn) match {
        case Success(_) =>
          true
        case Failure(e) =>
          logger.error(s"Error creating table $fullTableName", e)
          throw e
      }
    }
  }

  def addSCD2Columns(connection: Connection, engineName: Engine): Unit = {
    this.taskDesc.writeStrategy match {
      case Some(strategyOptions) if strategyOptions.getEffectiveType() == WriteStrategyType.SCD2 =>
        val startTsCol = strategyOptions.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
        val endTsCol = strategyOptions.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)
        val scd2Columns = List(startTsCol, endTsCol)
        val alterTableSqls = scd2Columns.map { column =>
          if (engineName.toString.toLowerCase() == "redshift")
            s"ALTER TABLE $fullTableName ADD COLUMN $column TIMESTAMP"
          else
            s"ALTER TABLE $fullTableName ADD COLUMN IF NOT EXISTS $column TIMESTAMP NULL"
        }
        // ignore errors if columns already exist
        Try(runSqls(connection, alterTableSqls, "addSCE2Columns"))
      case _ =>
    }
  }
  override protected lazy val sinkConnection: Settings.Connection = {
    if (interactive.isDefined) {
      JdbcDbUtils.readOnlyConnection(
        settings.appConfig.connections(sinkConnectionRef)
      )(settings)
    } else {
      settings.appConfig.connections(sinkConnectionRef)
    }
  }

  def runJDBC(df: Option[DataFrame]): Try[JdbcJobResult] = {
    val start = Timestamp.from(Instant.now())

    if (interactive.isEmpty && settings.appConfig.createSchemaIfNotExists) {
      // Creating a schema requires its own connection if called before a Spark save
      JdbcDbUtils.withJDBCConnection(sinkConnection.options) { conn =>
        JdbcDbUtils.createSchema(conn, fullDomainName)
      }
    }

    val res = Try {
      val mainSql =
        if (df.isEmpty) {
          buildAllSQLQueries(None)
        } else {
          val sql = taskDesc.getSql()
          val mainSql = schemaHandler.substituteRefTaskMainSQL(
            sql,
            taskDesc.getRunConnection(),
            allVars
          )
          mainSql
        }
      val sinkOptions =
        if (sinkConnection.isDuckDb()) {
          val duckDbEnableExternalAccess =
            settings.appConfig.duckDbEnableExternalAccess || sinkConnection.isMotherDuckDb()
          sinkConnection.options.updated(
            "enable_external_access",
            duckDbEnableExternalAccess.toString
          )
        } else {
          sinkConnection.options
        }

      interactive match {
        case Some(_) =>
          JdbcDbUtils.withJDBCConnection(sinkOptions) { conn =>
            runInteractive(conn, mainSql)
          }
        case None =>
          val parsedPreActions =
            Utils.parseJinja(
              jdbcSinkEngine.preActions.getOrElse(""),
              Map("schema" -> taskDesc.domain)
            )
          df match {
            case Some(loadedDF) =>
              JdbcDbUtils.withJDBCConnection(sinkOptions) { conn =>
                Try {
                  conn.setAutoCommit(false)
                  runPreActions(conn, parsedPreActions.splitSql(";"))
                  runSqls(conn, preSql, "Pre")
                  logger.info(s"Writing dataframe to $fullTableName")
                  val saveMode = strategy.toWriteMode().toSaveMode
                  if (saveMode == SaveMode.Overwrite || truncate) {
                    val jdbcUrl = sinkConnection.options("url")
                    // val dialect = SparkUtils.dialect(jdbcUrl)
                    // We always append to the table to keep the schema (Spark loose the schema otherwise). We truncate using the truncate query option
                    JdbcDbUtils.truncateTable(conn, fullTableName)
                  }
                } match {
                  case Success(_) =>
                    conn.commit()
                  case Failure(e) =>
                    conn.rollback()
                    throw e
                }
              }
              val tablePath = new Path(s"${settings.appConfig.datasets}/${fullTableName}")

              if (settings.storageHandler().exists(tablePath)) {
                settings.storageHandler().delete(tablePath)
              }
              if (sinkConnection.isDuckDb()) {
                loadedDF.write
                  .format("parquet")
                  .mode(SaveMode.Overwrite) // truncate done above if requested
                  .save(tablePath.toString)
                JdbcDbUtils.withJDBCConnection(
                  sinkConnection.options.updated("enable_external_access", "true")
                ) { conn =>
                  val sql =
                    s"INSERT INTO $fullTableName SELECT * FROM '$tablePath/*.parquet'"
                  JdbcDbUtils.executeUpdate(sql, conn)
                }
                settings.storageHandler().delete(tablePath)
              } else {
                loadedDF.write
                  .format(sinkConnection.sparkDatasource().getOrElse("jdbc"))
                  .option("dbtable", fullTableName)
                  .mode(SaveMode.Append) // truncate done above if requested
                  .options(sinkConnection.options)
                  .save()
              }

            case None =>
              JdbcDbUtils.withJDBCConnection(sinkOptions) { conn =>
                Try {
                  conn.setAutoCommit(false)
                  runPreActions(conn, parsedPreActions.splitSql(";"))
                  runSqls(conn, preSql, "Pre")
                  val finalSqls = mainSql.splitSql()
                  runSqls(conn, finalSqls, "Main")
                } match {
                  case Success(_) =>
                    conn.commit()
                  case Failure(e) =>
                    conn.rollback()
                    throw e
                }
              }
          }

          JdbcDbUtils.withJDBCConnection(sinkOptions) { conn =>
            Try {
              if (!test) applyJdbcAcl(conn, forceApply = true)
              runSqls(conn, postSql, "Post")
              addSCD2Columns(conn, sinkConnection.getJdbcEngineName())
            } match {
              case Success(_) =>
              case Failure(e) =>
                conn.rollback()
                throw e
            }
          }

          if (settings.appConfig.expectations.active) {
            new ExpectationJob(
              Option(applicationId()),
              taskDesc.database,
              taskDesc.domain,
              taskDesc.table,
              taskDesc.expectations,
              storageHandler,
              schemaHandler,
              new JdbcExpectationAssertionHandler(sinkOptions)
            ).run()
          }
          if (settings.appConfig.autoExportSchema) {
            val isTableInAuditDomain =
              taskDesc.domain == settings.appConfig.audit.getDomain()
            if (isTableInAuditDomain)
              logger.info(
                s"Table ${taskDesc.domain}.${taskDesc.table} is in audit domain, skipping schema extraction"
              )
            else {
              val config = ExtractSchemaConfig(
                external = true,
                outputDir = Some(DatasetArea.external.toString),
                tables = s"${taskDesc.domain}.${taskDesc.table}" :: Nil,
                connectionRef = Some(sinkConnectionRef),
                accessToken = accessToken
              )
              ExtractSchemaCmd.run(config, schemaHandler)
            }
          }
          JdbcJobResult(Nil)
      }
    }
    val end = Timestamp.from(Instant.now())
    res match {
      case Success(_) =>
        if (logExecution)
          logAuditSuccess(start, end, -1, test)
      case Failure(e) =>
        logAuditFailure(start, end, e, test)
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
      var rowCount = 0
      while (rs.next && rowCount < settings.appConfig.maxInteractiveRecords) {
        val rowAsSeq = new ListBuffer[String]
        var i = 1
        while (i <= rs.getMetaData.getColumnCount) {
          rowAsSeq.append(Option(rs.getObject(i)).map(_.toString).getOrElse("NULL"))
          i += 1
        }
        result.append(rowAsSeq.toList)
        rowCount += 1
      }
      JdbcJobResult(headerAsSeq.toList, result.toList)
    } catch {
      case e: Exception =>
        logger.error(s"Error running interactive query $mainSql", e)
        throw e
    } finally {
      stmt.close()
    }
  }

  lazy val fullTableName = s"$fullDomainName.${taskDesc.table}"

  private def runPreActions(conn: java.sql.Connection, preActions: List[String]): Unit = {
    runSqls(conn, preActions, "PreActions")
  }

  @throws[Exception]
  private def runSqls(conn: Connection, sqls: List[String], typ: String): Unit = {
    if (sqls.nonEmpty) {
      sqls.foreach { req =>
        JdbcDbUtils.executeUpdate(req, conn) match {
          case Success(_) =>
          case Failure(e) =>
            throw e
        }
      }
    }
  }

  def updateJdbcTableSchema(incomingSchema: StructType, tableName: String): Unit = {
    // update target table schema if needed
    val isSCD2 = strategy.getEffectiveType() == WriteStrategyType.SCD2
    val incomingSchemaWithSCD2 =
      if (isSCD2) {
        incomingSchema
          .add(
            StructField(
              strategy.startTs
                .getOrElse(settings.appConfig.scd2StartTimestamp),
              TimestampType,
              nullable = true
            )
          )
          .add(
            StructField(
              strategy.endTs.getOrElse(settings.appConfig.scd2EndTimestamp),
              TimestampType,
              nullable = true
            )
          )
      } else
        incomingSchema
    val sinkConnectionRefOptions = sinkConnection.options
    val jdbcUrl = sinkConnectionRefOptions("url")
    val targetTableExists: Boolean = tableExists
    JdbcDbUtils.withJDBCConnection(sinkConnectionRefOptions) { conn =>
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
            incomingSchemaWithSCD2,
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
          SparkUtils.alterTableAddColumnsString(addedSchema, tableName, Map.empty)
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
          optionsWrite,
          attDdl()
        )
      }
    }
  }
}

object JdbcAutoTask extends StrictLogging {
  def executeUpdate(sql: String, connectionName: String)(implicit
    settings: Settings
  ): Try[Boolean] = {
    val connection = settings.appConfig
      .connection(connectionName)
      .getOrElse(throw new Exception(s"Connection not found $connectionName"))

    // This is an update statement. No readonly connection is needed
    JdbcDbUtils.withJDBCConnection(connection.options) { conn =>
      JdbcDbUtils.execute(sql, conn)
    }
  }
}
