package ai.starlake.extract

import ai.starlake.config.{DatasetArea, Settings}
import better.files.File
import org.apache.hadoop.fs.Path

abstract class Extract {

  protected def mappingPath(mapping: String)(implicit settings: Settings): Path = {
    if (mapping.contains("/"))
      new Path(mapping)
    else {
      val mappingFilename =
        if (mapping.endsWith(".yml")) mapping else mapping + ".sl.yml"
      val paths =
        settings
          .storageHandler()
          .list(DatasetArea.extract, extension = ".yml", recursive = false)
          .map(_.path)

      paths.find(_.getName() == mappingFilename).getOrElse(new Path(mappingFilename))
    }
  }
  protected def schemaOutputDir(outputDir: Option[String])(implicit settings: Settings): File =
    File(outputDir.getOrElse(DatasetArea.load.toString))

  protected def dataOutputDir(outputDir: Option[String])(implicit settings: Settings): File =
    File(outputDir.getOrElse(DatasetArea.extract.toString))

}
