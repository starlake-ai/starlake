package ai.starlake.job.convert

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.utils.{JdbcJobResult, JobBase, JobResult}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

/** Convert parquet files to CSV using DuckDB.
  */
class Parquet2CSV(config: Parquet2CSVConfig, val storageHandler: StorageHandler)(implicit
  val settings: Settings
) extends JobBase
    with LazyLogging {

  override def name: String = s"parquet-2-csv"

  override def run(): Try[JobResult] = {
    val allPaths = (config.domainName, config.schemaName) match {
      case (Some(domainName), Some(schemaName)) =>
        List(new Path(new Path(config.inputFolder, domainName), schemaName))
      case (Some(domainName), None) =>
        storageHandler.listDirectories(new Path(config.inputFolder, domainName))
      case (None, None) =>
        storageHandler
          .listDirectories(config.inputFolder)
          .flatMap(domainPath => storageHandler.listDirectories(domainPath))
      case (None, Some(_)) => throw new Exception("Should never happen!")
    }
    val outputPath = config.outputFolder match {
      case None         => config.inputFolder
      case Some(folder) => folder
    }
    val connectionOptions = settings.appConfig.getDefaultConnection().options
    allPaths.flatMap { path =>
      val fileFound = Try {
        storageHandler.list(path, recursive = false).nonEmpty ||
        storageHandler.listDirectories(path).nonEmpty
      }.getOrElse(false)
      if (fileFound) {
        Try {
          val csvPath =
            new Path(new Path(outputPath, path.getParent.getName()), path.getName() + ".csv")
          val inputPath = StorageHandler.localFile(path).pathAsString
          val outputCsvPath = StorageHandler.localFile(csvPath).pathAsString
          JdbcDbUtils.withJDBCConnection(None, connectionOptions) { conn =>
            val sql =
              s"COPY (SELECT * FROM read_parquet('$inputPath')) TO '$outputCsvPath' (FORMAT CSV, HEADER)"
            JdbcDbUtils.execute(sql, conn)
          }
          if (config.deleteSource)
            storageHandler.delete(path)
          Some(csvPath)
        } match {
          case Success(result) => Option(result)
          case Failure(e) =>
            e.printStackTrace()
            None
        }
      } else {
        None
      }
    }
    Success(JdbcJobResult(Nil))
  }
}

object Parquet2CSV {

  def main(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(Settings.referenceConfig, None, None, None, None)

    Parquet2CSVCmd.run(args.toIndexedSeq, settings.schemaHandler())
  }
}
