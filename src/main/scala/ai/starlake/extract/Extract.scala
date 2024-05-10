package ai.starlake.extract

import ai.starlake.config.{DatasetArea, Settings}
import org.apache.hadoop.fs.Path

abstract class Extract {

  protected def mappingPath(area: Path, mapping: String)(implicit settings: Settings): Path = {
    if (mapping.contains("/"))
      new Path(mapping)
    else {
      val mappingFilename =
        if (mapping.endsWith(".yml")) mapping else mapping + ".sl.yml"
      val paths =
        settings
          .storageHandler()
          .list(area, extension = ".yml", recursive = false)
          .map(_.path)

      paths.find(_.getName() == mappingFilename).getOrElse(new Path(mappingFilename))
    }
  }
  protected def schemaOutputDir(outputDir: Option[String])(implicit settings: Settings): Path =
    new Path(outputDir.getOrElse(DatasetArea.load.toString))

  protected def dataOutputDir(outputDir: Option[String])(implicit settings: Settings): Path =
    new Path(outputDir.getOrElse(DatasetArea.extract.toString))

}
