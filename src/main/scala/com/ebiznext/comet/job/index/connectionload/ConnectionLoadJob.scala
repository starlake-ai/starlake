package com.ebiznext.comet.job.index.connectionload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import org.apache.spark.sql.SaveMode

import scala.util.Try

class ConnectionLoadJob(
  cliConfig: ConnectionLoadConfig
)(implicit val settings: Settings)
    extends SparkJob {

  /** Set extra spark conf here otherwise it will be too late once the spark session is created.
    * @return
    */
  override def withExtraSparkConf(): Map[String, String] = cliConfig.options

  override def name: String = s"jdbcload-JDBC-${cliConfig.outputTable}"

  val conf = session.sparkContext.hadoopConfiguration
  logger.info(s"JDBC Config $cliConfig")

  def runJDBC(): Try[SparkJobResult] = {
    val inputPath = cliConfig.sourceFile
    logger.info(s"Input path $inputPath")
    Try {
      val sourceDF =
        inputPath match {
          case Left(path) => session.read.parquet(path)
          case Right(df)  => df
        }

      // Some database do not suport truncate during save
      // Truncate should be done manually in pre-sql
      // https://stackoverflow.com/questions/59451275/how-to-generate-a-spark-sql-truncate-query-without-only
      val writeMode =
        if (cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE) SaveMode.Overwrite
        else SaveMode.Append
      val dfw = sourceDF.write
        .format(cliConfig.format)
        .option("truncate", cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE)
        .option("dbtable", cliConfig.outputTable)
        .mode(cliConfig.mode.getOrElse(writeMode.toString))

      cliConfig.options.foldLeft(dfw) { case (w, (k, v)) => w.option(k, v) }.save()
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
