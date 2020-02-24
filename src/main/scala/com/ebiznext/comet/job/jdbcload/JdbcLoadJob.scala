package com.ebiznext.comet.job.jdbcload

import java.sql.DriverManager

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.{SparkJob, Utils}
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

class JdbcLoadJob(
  cliConfig: JdbcLoadConfig
)(implicit val settings: Settings) extends SparkJob {

  override def name: String = s"jdbcload-JDBC-${cliConfig.outputTable}"

  val conf = session.sparkContext.hadoopConfiguration
  logger.info(s"JDBC Config $cliConfig")
  val driver = cliConfig.driver
  val url = cliConfig.url
  val user = cliConfig.user
  val password = cliConfig.password
  Class.forName(driver)

  def getOrCreateTables() = {
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement
      val tables = List("jdbc-audit-table", "jdbc-rejected-table" /*, "jdbc-metrics-table" */)
      tables.foreach { table =>
        // FIXME: find a way to locate the name of the table, test whether it exists, and if not THEN attempt creation
        val sqlCreateTable = Settings.comet.audit.options.get(table).format(cliConfig.outputTable)
        stmt.executeUpdate(sqlCreateTable)
      }
      stmt.close()
    } finally {
      conn.close()
    }
  }

  def runJDBC(): Try[SparkSession] = {
    val inputPath = cliConfig.sourceFile
    logger.info(s"Input path $inputPath")
    Try {
      val sourceDF =
        inputPath match {
          case Left(path) => session.read.parquet(path)
          case Right(df)  => df
        }
      sourceDF.write
        .format("jdbc")
        .option("numPartitions", cliConfig.partitions)
        .option("batchsize", cliConfig.batchSize)
        .option("truncate", cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE)
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", cliConfig.outputTable)
        .option("user", user)
        .option("password", password)
        .mode(SaveMode.Append)
        .save()
      session
    }
  }

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): Try[SparkSession] = {
    val res =
      if (Settings.comet.audit.active && Settings.comet.audit.index == "JDBC")
        runJDBC()
      else
        Success(session)
    res match {
      case Success(_)         =>
      case Failure(exception) => Utils.logException(logger, exception)
    }
    res
  }
}
