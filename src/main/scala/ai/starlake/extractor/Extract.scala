package ai.starlake.extractor

import ai.starlake.config.{DatasetArea, Settings}
import better.files.File
import org.apache.hadoop.fs.Path

abstract class Extract {

  protected def mappingPath(mapping: String)(implicit settings: Settings): Path = {
    val mappingFilename =
      if (mapping.endsWith(".yml")) mapping else mapping + ".comet.yml"
    val paths =
      settings.storageHandler.list(DatasetArea.extract, extension = ".yml", recursive = false)

    paths.find(_.getName() == mappingFilename).getOrElse(new Path(mappingFilename))

  }

  protected def outputDir(outputDir: Option[String])(implicit settings: Settings): File =
    File(outputDir.getOrElse(DatasetArea.extract.toString))

}
