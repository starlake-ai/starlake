package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.extract.JDBCUtils
import ai.starlake.job.metrics.{ExpectationJob, JdbcExpectationAssertionHandler}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AccessControlEntry, AutoTaskDesc}
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{JdbcJobResult, JobResult}
import org.apache.spark.sql.SaveMode

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
    val databaseMetadata = conn.getMetaData()
    val rs =
      databaseMetadata.getTables(
        taskDesc.database.orNull,
        taskDesc.domain.toUpperCase(), // JDBC require tables and schema in uppercase
        taskDesc.table.toUpperCase(),
        Array[String]("TABLE")
      )
    val ok = rs.next()
    rs.close()
    if (!ok && taskDesc._auditTableName.isDefined) {
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
      JDBCUtils.applyScript(s"CREATE SCHEMA IF NOT EXISTS $fullDomainName", conn)

      val script = scriptTemplate.richFormat(
        Map("table" -> fullTableName),
        Map.empty
      )
      JDBCUtils.applyScript(script, conn)
    }
    ok
  }

  def runSqls(conn: Connection, sqls: List[String]): Unit = {
    sqls.foreach { req =>
      val stmt = conn.createStatement()
      try {
        stmt.execute(req)
      } finally {
        stmt.close()
      }
    }

  }
  def runJDBC(): Try[JdbcJobResult] = {
    val start = Timestamp.from(Instant.now())
    val res = Try {
      JDBCUtils.withJDBCConnection(connection.options) { conn =>
        val dynamicPartitionOverwrite = None // No partition in snowflake
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
              val rowAsSeq = new ListBuffer[String]
              result.append(rowAsSeq.toList)
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
            runSqls(conn, preSql)
            runMainSql(sqlWithParameters, conn)
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
            runSqls(conn, postSql)
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

  def isMerge(sql: String) = sql.toLowerCase().contains("merge into")
  private def runMainSql(
    sqlWithParameters: String,
    conn: java.sql.Connection
  ): Unit = {

    // TODO: Make optional DOMAIN creation
    JDBCUtils.applyScript(s"CREATE SCHEMA IF NOT EXISTS $fullDomainName", conn)
    val materializedView = taskDesc.sink.flatMap(_.materializedView).getOrElse(false)
    val finalSqls =
      if (!tableExists(conn)) { // We are sure that there is no merge in the sql belows
        if (materializedView)
          List(s"CREATE MATERIALIZED VIEW $fullTableName AS $sqlWithParameters")
        else
          List(s"CREATE TABLE $fullTableName AS $sqlWithParameters")

      } else {
        val mainSql =
          if (isMerge(sqlWithParameters))
            sqlWithParameters
          else
            s"INSERT INTO $fullTableName $sqlWithParameters"
        val insertSqls =
          if (taskDesc.getWrite().toSaveMode == SaveMode.Overwrite) {
            if (materializedView) {
              List(
                s"DROP MATERIALIZED VIEW $fullTableName",
                s"CREATE MATERIALIZED VIEW $fullTableName AS $sqlWithParameters"
              )
            } else {
              List(
                s"TRUNCATE TABLE $fullTableName",
                mainSql
              )
            }
          } else {
            val dropSqls =
              if (truncate)
                List(s"TRUNCATE TABLE $fullTableName")
              else
                Nil
            dropSqls ++ List(mainSql)
          }
        insertSqls
      }
    finalSqls.foreach(sql => JDBCUtils.applyScript(sql, conn))
    applyJdbcAcl(connection, forceApply = true)
  }
}
