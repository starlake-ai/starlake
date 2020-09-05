package com.ebiznext.comet.job.convert

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.WriteMode
import com.ebiznext.comet.utils.SparkJob
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import scala.util.{Success, Try}

/**
  * Convert parquet files to CSV.
  * The folder hierarchy should be in the form /input_folder/domain/schema/part*.parquet
  * Once converted the csv files is put in the folder /output_folder/domain/schema.csv file
  * When the specified number of parittions is 1 then /output_folder/domain/schema.csv is the file containing the data
  * otherwise, it is a folder containing the part*.csv files.
  * When output_folder is not specified, then the input_folder is used a the base output folder.
  * @param config
  * @param storageHandler
  * @param settings
  */
class Parquet2CSV(config: Parquet2CSVConfig, val storageHandler: StorageHandler)(implicit
  val settings: Settings
) extends SparkJob {

  override def name: String = s"parquet-2-csv"

  override def run(): Try[Option[DataFrame]] = {
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
      val successPath = new Path(path, "_SUCCESS")
      storageHandler.exists(successPath) match {
        case true =>
          val csvPath =
            new Path(new Path(outputPath, path.getParent.getName()), path.getName() + ".csv")
          val writer = session.read
            .parquet(path.toString)
            .coalesce(config.partitions)
            .write
            .mode(config.writeMode.getOrElse(WriteMode.ERROR_IF_EXISTS).toSaveMode)
          config.options
            .foldLeft(writer)((w, kv) => w.option(kv._1, kv._2))
            .option("ignoreLeadingWhiteSpace", false)
            .option("ignoreTrailingWhiteSpace", false)
            .csv(csvPath.toString)
          if (config.partitions == 1) {
            val files = storageHandler.list(csvPath, "csv")
            files.foreach { f =>
              val tmpFile = new Path(csvPath.getParent, csvPath.getName + ".tmp")
              storageHandler.move(f, tmpFile)
              storageHandler.delete(f)
              storageHandler.delete(csvPath)
              storageHandler.move(tmpFile, csvPath)
            }
          }
          if (config.deleteSource)
            storageHandler.delete(path)
          Some(csvPath)
        case false =>
          None
      }
    }
    Success(None)
  }
}

object Parquet2CSV {

  def main(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    settings.publishMDCData()

    import settings.storageHandler
    Parquet2CSVConfig.parse(args) match {
      case Some(config) =>
        new Parquet2CSV(config, storageHandler).run()
      case _ =>
        println(Parquet2CSVConfig.usage())
    }
  }
}
