package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.metrics.{ExpectationJob, JdbcExpectationAssertionHandler}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AccessControlEntry, AutoTaskDesc}
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{JdbcJobResult, JobResult, SparkUtils, Utils}
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

  def tableExists(conn: java.sql.Connection): Boolean = {
    val url = connection.options("url")
    JdbcDbUtils.tableExists(conn, url, fullTableName)
  }

  private def prepareTarget(conn: java.sql.Connection, exists: Boolean): Boolean = {
    if (!exists) {
      if (taskDesc._auditTableName.isDefined) {
        // Table not found and it is an table in the audit schema defined in the reference-connections.conf file  Try to create it.
        logger.info(s"Table ${taskDesc.table} not found in ${taskDesc.domain}")
        val connectionRef =
          this.sinkConfig.flatMap(_.connectionRef).getOrElse(settings.appConfig.connectionRef)

        val jdbcEngineName = settings.appConfig.connections(connectionRef).getJdbcEngineName()
        val engine = settings.appConfig.jdbcEngines(jdbcEngineName.toString)
        val entry = taskDesc._auditTableName.getOrElse(
          throw new Exception(
            s"audit table for output ${taskDesc.table} is not defined in engine $jdbcEngineName"
          )
        )
        val scriptTemplate = engine.tables(entry).createSql
        JdbcDbUtils.createSchema(fullDomainName, conn)

        val script = scriptTemplate.richFormat(
          Map("table" -> fullTableName),
          Map.empty
        )
        JdbcDbUtils.execute(script, conn) match {
          case Success(_) =>
          case Failure(e) =>
            logger.error(s"Error creating table $fullTableName", e)
            throw e
        }
      }
    }
    exists
  }

  @throws[Exception]
  def runSqls(conn: Connection, sqls: List[String]): Unit = {
    sqls.foreach { req =>
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
      JdbcDbUtils.withJDBCConnection(connection.options) { conn =>
        val dynamicPartitionOverwrite = None // No partition in snowflake / redshift / postgresql
        val (preSql, sqlWithParameters, postSql, asTable) =
          buildAllSQLQueries(tableExists(conn), dynamicPartitionOverwrite, None, Nil)
        logger.info(s"""START COMPILE SQL $sqlWithParameters END COMPILE SQL""")
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
                  val jdbcDialect = SparkUtils.dialect(url)
                  logger.debug(s"JDBC dialect $jdbcDialect")
                  jdbcDialect
                case None =>
                  logger.warn("No url found in jdbc options. Using default dialect")
                  new JdbcDialect {
                    override def canHandle(url: String): Boolean = true
                  }
              }
            runSqls(conn, preSql)
            runMainSql(sqlWithParameters, conn, jdbcDialect)
            runSqls(conn, postSql)
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

  def isMerge(sql: String) = {
    sql.toLowerCase().contains("merge into")
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
    prepareTarget(conn, tableExists(conn))
    val finalSqls =
      if (!tableExists(conn)) { // Table may have been created by the preapreTarget method so we test it again
        // If table does not exist we know for sure that the sql request is a SELECT
        if (materializedView)
          List(s"CREATE MATERIALIZED VIEW $fullTableName AS $sqlWithParameters")
        else
          List(s"CREATE TABLE $fullTableName AS $sqlWithParameters")

      } else {
        val mainSql =
          if (isMerge(sqlWithParameters))
            sqlWithParameters // it's a merge request.
          else {
            taskDesc._auditTableName match {
              case Some(_) =>
                s"INSERT INTO $fullTableName $sqlWithParameters"
              case None =>
                // We will be inserting the resulting data into the target table.
                val start = sqlWithParameters.indexOf("SELECT ") + "SELECT ".length
                var end = sqlWithParameters.indexOf(" FROM(")
                if (end < 0)
                  end = sqlWithParameters.indexOf(" FROM ")
                val columns = sqlWithParameters.substring(start, end)
                s"INSERT INTO $fullTableName($columns) $sqlWithParameters"

            }
          }
        val insertSqls =
          if (taskDesc.getWrite().toSaveMode == SaveMode.Overwrite) {
            // If we are in overwrite mode we need to drop the table/truncate before inserting
            if (materializedView) {
              List(
                s"DROP MATERIALIZED VIEW $fullTableName",
                s"CREATE MATERIALIZED VIEW $fullTableName AS $sqlWithParameters"
              )
            } else {
              List(
                jdbcDialect.getTruncateQuery(fullTableName),
                mainSql
              )
            }
          } else {
            val dropSqls =
              if (truncate)
                List(jdbcDialect.getTruncateQuery(fullTableName))
              else
                Nil
            dropSqls ++ List(mainSql)
          }
        insertSqls
      }
    logger.info(s"running against te database: $finalSqls")
    val results = finalSqls.map(sql => JdbcDbUtils.execute(sql, conn))
    results.filter(_.isFailure).foreach(_.failed.foreach(e => Utils.logException(logger, e)))
    applyJdbcAcl(connection, forceApply = true)
  }
}
