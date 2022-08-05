package ai.starlake.job.sink.jdbc

import ai.starlake.config.Settings
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import org.apache.spark.sql.SaveMode

import scala.util.Try

class ConnectionLoadJob(
  cliConfig: ConnectionLoadConfig
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = s"jdbcload-JDBC-${cliConfig.outputTable}"

  val conf = session.sparkContext.hadoopConfiguration
  logger.info(s"JDBC Config $cliConfig")

  def runJDBC(): Try[SparkJobResult] = {
    val inputPath = cliConfig.sourceFile
    logger.info(s"Input path $inputPath")
    Try {
      val sourceDF =
        inputPath match {
          case Left(path) => session.read.format(settings.comet.defaultFormat).load(path)
          case Right(df)  => df
        }

      // Some database do not support truncate during save
      // Truncate should be done manually in pre-sql
      // https://stackoverflow.com/questions/59451275/how-to-generate-a-spark-sql-truncate-query-without-only
      val writeMode =
        if (cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE) SaveMode.Overwrite
        else SaveMode.Append
      val dfw = sourceDF.write
        .format(cliConfig.format)

      val finalDfw =
        if (cliConfig.format == "jdbc")
          dfw
            .option("truncate", cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE)
            .option("dbtable", cliConfig.outputTable)
        else
          dfw

      finalDfw
        .mode(cliConfig.mode.getOrElse(writeMode.toString))
        .options(cliConfig.options)
        .save()
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
