package com.ebiznext.comet.job.convert

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.utils.SparkJob
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.util.{Success, Try}

class Parquet2CSV(config: Parquet2CSVConfig, val storageHandler: StorageHandler)(
  implicit val settings: Settings
) extends SparkJob {

  override def name: String = s"parquet-2-csv"

  override def run(): Try[SparkSession] = {
    val allPaths = (config.domainName, config.schemaName) match {
      case (Some(domainName), Some(schemaName)) =>
        List(new Path(new Path(config.inputFolder, domainName), schemaName))
      case (Some(domainName), None) =>
        storageHandler.list(new Path(config.inputFolder, domainName))
      case (None, None) =>
        storageHandler
          .list(config.inputFolder)
          .flatMap(domainPath => storageHandler.list(domainPath))
    }
    val outputPath = config.outputFolder match {
      case None         => config.inputFolder
      case Some(folder) => folder
    }
    allPaths.flatMap { path: Path =>
        val successPath = new Path(path, "_SUCCESS")
      storageHandler.exists(successPath) match {
        case true =>
          val csvPath = new Path(outputPath, path.getName() + ".csv")
          session.read
            .parquet(path.toString)
            .coalesce(config.partitions.getOrElse(1))
            .write
            .option("header", String.valueOf(config.withHeader.getOrElse(true)))
            .save(csvPath.toString)
          if (config.partitions.getOrElse(1) == 1) {
            val files = storageHandler.list(csvPath, "parquet")
            files.foreach { f =>
              val tmpFile = new Path(csvPath.getParent, csvPath.getName + ".tmp")
              storageHandler.move(f, tmpFile)
              storageHandler.delete(f)
              storageHandler.delete(csvPath)
              storageHandler.move(tmpFile, csvPath)
            }
          }
          Some(csvPath)
        case false =>
          None
      }
    }
    Success(session)
  }
}

object Parquet2CSV {
  def main(args:Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    settings.publishMDCData()

    import settings.{storageHandler}
    Parquet2CSVConfig.parse(args) match {
      case Some(config) =>
        new Parquet2CSV(config, storageHandler)
      case _ =>
        println(Parquet2CSVConfig.usage())
    }
  }
}