package ai.starlake.job.convert

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.WriteMode.ERROR_IF_EXISTS
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult}
import org.apache.hadoop.fs.Path

import scala.util.{Success, Try}

/** Convert parquet files to CSV. The folder hierarchy should be in the form
  * /input_folder/domain/schema/part*.parquet Once converted the csv files is put in the folder
  * /output_folder/domain/schema.csv file When the specified number of parittions is 1 then
  * /output_folder/domain/schema.csv is the file containing the data otherwise, it is a folder
  * containing the part*.csv files. When output_folder is not specified, then the input_folder is
  * used a the base output folder.
  * @param config
  * @param storageHandler
  * @param settings
  */
class Parquet2CSV(config: Parquet2CSVConfig, val storageHandler: StorageHandler)(implicit
  val settings: Settings
) extends SparkJob {

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
    allPaths.flatMap { path: Path =>
      val fileFound = Try {
        storageHandler.list(path, recursive = false).nonEmpty ||
        storageHandler.listDirectories(path).nonEmpty
      }.getOrElse(false)
      if (fileFound) {
        Try {
          val csvPath =
            new Path(new Path(outputPath, path.getParent.getName()), path.getName() + ".csv")
          val writer = session.read
            .format(settings.appConfig.defaultWriteFormat)
            .load(path.toString)
            .repartition(config.partitions)
            .write
            .mode(config.writeMode.getOrElse(ERROR_IF_EXISTS).toSaveMode)
          writer
            .options(config.options.toMap)
            .option("ignoreLeadingWhiteSpace", false)
            .option("ignoreTrailingWhiteSpace", false)
            .csv(csvPath.toString)
          if (config.partitions == 1) {
            storageHandler.moveSparkPartFile(csvPath, ".csv")
          }
          if (config.deleteSource)
            storageHandler.delete(path)
          Some(csvPath)
        }.getOrElse(None)
      } else {
        None
      }
    }
    Success(SparkJobResult(None))
  }
}

object Parquet2CSV {

  def main(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(Settings.referenceConfig)

    import settings.storageHandler
    Parquet2CSVConfig.parse(args) match {
      case Some(config) =>
        new Parquet2CSV(config, storageHandler()).run()
      case _ =>
    }
  }
}
