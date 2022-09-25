package ai.starlake.job.convert

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult}

import scala.util.{Success, Try}

class FileSplitter(config: FileSplitterConfig, val storageHandler: StorageHandler)(implicit
  val settings: Settings
) extends SparkJob {
  override def name: JdbcConfigName = "file-splitter"

  override def run(): Try[JobResult] = {
    config match {
      case FileSplitterConfig(_, _, split, Some(delimiter), None, None) =>
        val df = session.read
          .option("delimiter", delimiter)
          .csv(config.inputFile.toString)

        df
          .withColumnRenamed(df.columns.head, split)
          .write
          .partitionBy(split)
          .csv(config.outputFolder.toString)
      case FileSplitterConfig(_, _, split, None, Some(start), Some(end)) =>
        val df = session.read
          .text(config.inputFile.toString)
        df.createOrReplaceTempView("table")
        session
          .sql(s"select value, substr(value, $start, (${end + 1} - $start)) as $split from table")
          .write
          .partitionBy(split)
          .text(config.outputFolder.toString)
      case _ =>
        throw new Exception("Should never happen")
    }
    Success(SparkJobResult(None))
  }
}
