package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.metrics.{ExpectationJob, JdbcExpectationAssertionHandler}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AccessControlEntry, AutoTaskDesc, Engine}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{JdbcJobResult, JobResult, Utils}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.jdbc.JdbcDialect

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

  val jdbcEngineName = this.connection.getJdbcEngineName()
  val jdbcEngine = settings.appConfig.jdbcEngines(jdbcEngineName.toString)

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
    runJDBC()
  }

  lazy val tableExists: Boolean = {
    JdbcDbUtils.withJDBCConnection(connection) { conn =>
      JdbcDbUtils.tableExists(conn, connection.jdbcUrl, fullTableName)
    }
  }

  private def createAuditTable(conn: java.sql.Connection): Boolean = {
    // Table not found and it is an table in the audit schema defined in the reference-connections.conf file  Try to create it.
    logger.info(s"Table ${taskDesc.table} not found in ${taskDesc.domain}")
    val entry = taskDesc._auditTableName.getOrElse(
      throw new Exception(
        s"audit table for output ${taskDesc.table} is not defined in engine $jdbcEngineName"
      )
    )
    val scriptTemplate = jdbcEngine.tables(entry).createSql
    JdbcDbUtils.createSchema(fullDomainName, conn)

    val script = scriptTemplate.richFormat(
      Map("table" -> fullTableName, "writeFormat" -> settings.appConfig.defaultWriteFormat),
      Map.empty
    )
    JdbcDbUtils.execute(script, conn) match {
      case Success(_) =>
        true
      case Failure(e) =>
        logger.error(s"Error creating table $fullTableName", e)
        throw e
    }
  }

  @throws[Exception]
  def runSqls(conn: Connection, sqls: List[String]): Unit = {
    sqls.foreach { req =>
      logger.info(s"running sql request $req")
      JdbcDbUtils.execute(req, conn) match {
        case Success(_) =>
        case Failure(e) =>
          logger.error(s"Error running sql $req", e)
          throw e
      }
    }
  }

  def runJDBC(): Try[JdbcJobResult] = {
    val start = Timestamp.from(Instant.now())
    val res = Try {
      JdbcDbUtils.withJDBCConnection(connection) { conn =>
        val dynamicPartitionOverwrite = None // No partition in snowflake / redshift / postgresql
        val (preSql, sqlWithParameters, postSql, asTable) =
          buildAllSQLQueries(tableExists, dynamicPartitionOverwrite, None, Engine.JDBC, Nil)
        logger.info(s"""$sqlWithParameters""")
        logger.info(s"running sql request using JDBC driver")
        interactive match {
          case Some(_) =>
            val stmt = conn.createStatement()
            try {
              val rs = stmt.executeQuery(sqlWithParameters)
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
          case None =>
            val jdbcDialect =
              connection.options.get("url") match {
                case Some(url) =>
                  val jdbcDialect = connection.dialect
                  logger.debug(s"JDBC dialect $jdbcDialect")
                  jdbcDialect
                case None =>
                  logger.warn("No url found in jdbc options. Using default dialect")
                  new JdbcDialect {
                    override def canHandle(url: String): Boolean = true
                  }
              }
            conn.setAutoCommit(false)
            val parsedPreActions =
              Utils.parseJinja(jdbcEngine.preactions, Map("schema" -> taskDesc.domain))
            Try {
              runPreActions(conn, parsedPreActions.split(";").toList)
              runSqls(conn, preSql)
              runMainSql(sqlWithParameters, conn, jdbcDialect)
              runSqls(conn, postSql)
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
                None,
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

  val fullDomainName = taskDesc.database match {
    case Some(db) => s"$db.${taskDesc.domain}"
    case None     => taskDesc.domain
  }

  val fullTableName = s"$fullDomainName.${taskDesc.table}"

  private def isMerge(sql: String): Boolean = {
    sql.toLowerCase().contains("merge into")
  }

  private def runPreActions(conn: java.sql.Connection, preactions: List[String]): Unit = {
    runSqls(conn, preactions)
  }

  private def runMainSql(
    sqlWithParameters: String,
    conn: java.sql.Connection,
    jdbcDialect: JdbcDialect
  ): Unit = {

    // TODO: Make optional DOMAIN creation
    if (settings.appConfig.createSchemaIfNotExists)
      JdbcDbUtils.createSchema(fullDomainName, conn)
    val materializedView = taskDesc.sink.flatMap(_.materializedView).getOrElse(false)
    val tableCreated =
      if (!tableExists && taskDesc._auditTableName.isDefined) createAuditTable(conn)
      else tableExists
    val allSqls = sqlWithParameters.split(";\n")
    val preMainSqls = allSqls.dropRight(1).toList
    val lastSql = allSqls.last
    val finalSqls =
      if (!tableCreated) { // Table may have been created yet
        // If table does not exist we know for sure that the sql request is a SELECT
        if (materializedView)
          List(s"CREATE MATERIALIZED VIEW $fullTableName AS $lastSql")
        else
          List(s"CREATE TABLE $fullTableName AS $lastSql")

      } else {
        val mainSql =
          if (isMerge(sqlWithParameters)) {
            lastSql // it's a merge request.
          } else {
            taskDesc._auditTableName match {
              case Some(_) =>
                s"INSERT INTO $fullTableName $lastSql"
              case None =>
                // We will be inserting the resulting data into the target table.
                val columns = SQLUtils.extractColumnNames(lastSql).mkString(",")
                s"INSERT INTO $fullTableName($columns) $lastSql"

            }
          }
        val mainSqlList = mainSql.split(";\n").toList
        val insertSqls =
          if (taskDesc.getWrite().toSaveMode == SaveMode.Overwrite) {
            // If we are in overwrite mode we need to drop the table/truncate before inserting
            if (materializedView) {
              List(
                s"DROP MATERIALIZED VIEW $fullTableName",
                s"CREATE MATERIALIZED VIEW $fullTableName AS $lastSql"
              )
            } else {
              List(jdbcDialect.getTruncateQuery(fullTableName)) ++ mainSqlList
            }
          } else {
            val dropSqls =
              if (truncate)
                List(jdbcDialect.getTruncateQuery(fullTableName))
              else
                Nil
            dropSqls ++ mainSqlList
          }
        insertSqls
      }
    runSqls(conn, preMainSqls ++ finalSqls)
    applyJdbcAcl(connection, forceApply = true)
  }
}
