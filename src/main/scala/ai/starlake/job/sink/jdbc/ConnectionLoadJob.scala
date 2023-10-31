package ai.starlake.job.sink.jdbc

import ai.starlake.config.Settings
import ai.starlake.extract.JDBCUtils
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import org.apache.spark.sql.SaveMode

import scala.util.Try

class ConnectionLoadJob(
  cliConfig: JdbcConnectionLoadConfig
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = s"cnxload-JDBC-${cliConfig.outputTable}"

  val conf = session.sparkContext.hadoopConfiguration
  logger.info(s"JDBC Config $cliConfig")

  def runJDBC(): Try[SparkJobResult] = {
    val inputPath = cliConfig.sourceFile
    logger.info(s"Input path $inputPath")
    Try {
      val sourceDF =
        inputPath match {
          case Left(path) => session.read.format(settings.appConfig.defaultFormat).load(path)
          case Right(df)  => df
        }
      val outputDomain = cliConfig.outputTable.split("\\.")(0)
      val sql = s"CREATE SCHEMA IF NOT EXISTS $outputDomain"
      val jdbcOptions =
        if (cliConfig.format == "snowflake") {
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
      JDBCUtils.withJDBCConnection(jdbcOptions) { conn =>
        val stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
      }
      // Some database do not support truncate during save
      // Truncate should be done manually in pre-sql
      // https://stackoverflow.com/questions/59451275/how-to-generate-a-spark-sql-truncate-query-without-only
      val writeMode =
        if (cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE) SaveMode.Overwrite
        else SaveMode.Append
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
