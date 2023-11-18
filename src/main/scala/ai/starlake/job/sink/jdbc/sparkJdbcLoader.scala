package ai.starlake.job.sink.jdbc

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.utils._
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}

import java.sql.Connection
import scala.util.{Failure, Success, Try}

class sparkJdbcLoader(
  cliConfig: JdbcConnectionLoadConfig
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = s"cnxload-JDBC-${cliConfig.outputTable}"

  def tableExists(conn: java.sql.Connection, url: String, tableName: String): Boolean = {
    JdbcDbUtils.tableExists(conn, url, tableName)
  }

  val conf = session.sparkContext.hadoopConfiguration
  logger.info(s"JDBC Config $cliConfig")

  val jdbcOptions = {
    val options = if (cliConfig.format == "snowflake") {
      cliConfig.options.flatMap { case (k, v) =>
        if (k.startsWith("sf")) {
          val jdbcK = k.replace("sf", "").toLowerCase().replace("database", "db")
          val finalv =
            if (jdbcK == "url")
              "jdbc:snowflake://" + v
            else
              v
          List(
            jdbcK -> finalv,
            k     -> v
          )
        } else
          List(k -> v)

      }
    } else
      cliConfig.options
    CaseInsensitiveMap[String](options)
  }

  def runJDBC(): Try[SparkJobResult] = {
    val inputPath = cliConfig.sourceFile
    logger.info(s"Input path $inputPath")

    val writeMode =
      if (cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE) SaveMode.Overwrite
      else SaveMode.Append
    Try {
      val sourceDF =
        inputPath match {
          case Left(path) => session.read.format(settings.appConfig.defaultFormat).load(path)
          case Right(df)  => df
        }
      val outputDomain = cliConfig.outputTable.split("\\.")(0)
      val createSchemaSql = s"CREATE SCHEMA IF NOT EXISTS $outputDomain"
      JdbcDbUtils.withJDBCConnection(jdbcOptions) { conn =>
        JdbcDbUtils.execute(createSchemaSql, conn)
        if (writeMode == SaveMode.Overwrite)
          truncateTable(conn, jdbcOptions)
        val url = jdbcOptions("url")
        val exists = tableExists(conn, url, cliConfig.outputTable)
        val schema = sourceDF.schema
        if (SparkUtils.isFlat(schema) && exists) {
          val existingSchema = SparkUtils.getSchemaOption(conn, jdbcOptions, cliConfig.outputTable)
          val addedSchema = SparkUtils.added(schema, existingSchema.getOrElse(schema))
          val deletedSchema = SparkUtils.dropped(schema, existingSchema.getOrElse(schema))
          val alterTableDropColumns =
            SparkUtils.alterTableDropColumnsString(deletedSchema, cliConfig.outputTable)
          val alterTableAddColumns =
            SparkUtils.alterTableAddColumnsString(addedSchema, cliConfig.outputTable)
          alterTableDropColumns.foreach(JdbcDbUtils.executeAlterTable(_, conn))
          alterTableAddColumns.foreach(JdbcDbUtils.executeAlterTable(_, conn))
        } else {
          val optionsWrite = new JdbcOptionsInWrite(url, cliConfig.outputTable, jdbcOptions)
          SparkUtils.createTable(conn, cliConfig.outputTable, schema, false, optionsWrite)
        }
      }

      val dfw = sourceDF.write
        .format(cliConfig.format)
        .option("dbtable", cliConfig.outputTable)

      val finalDfw =
        if (cliConfig.format == "jdbc")
          dfw
            .option("truncate", cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE)
        else
          dfw

      finalDfw
        .mode(writeMode.toString)
        .options(cliConfig.options)
        .save()

      logger.info(s"JDBC save done to table ${cliConfig.outputTable} at ${cliConfig.options}")
      SparkJobResult(None)
    }
  }

  private def truncateTable(
    conn: Connection,
    jdbcOptions: Map[String, String]
  ): Unit = {
    // Some database do not support truncate during save
    // Truncate should be done manually in pre-sql
    // https://stackoverflow.com/questions/59451275/how-to-generate-a-spark-sql-truncate-query-without-only
    if (jdbcOptions.get("supportTruncateOnInsert").contains("false")) {
      val jdbcDialect = jdbcOptions.get("url") match {
        case Some(url) =>
          JdbcDialects.get(url)
        case None =>
          logger.warn("No url found in jdbc options. Using TRUNCATE TABLE")
          new JdbcDialect {
            override def canHandle(url: String): Boolean = true
          }
      }
      val truncateSql = jdbcDialect.getTruncateQuery(cliConfig.outputTable)

      // do not fail on exception. Truncate may fail is table does not exist
      JdbcDbUtils.execute(truncateSql, conn) match {
        case Failure(e) =>
          logger.warn(s"Truncate failed on table ${cliConfig.outputTable} with error $e")
        case Success(_) =>
          logger.info(s"Truncate done on table ${cliConfig.outputTable}")
      }
    }
  }

  /** Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    val res = runJDBC()
    Utils.logFailure(res, logger)
  }
}
