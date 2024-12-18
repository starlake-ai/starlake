package ai.starlake.extract

import ai.starlake.config.{DatasetArea, Settings}
import org.apache.hadoop.fs.Path

trait ExtractPathHelper {

  protected def mappingPath(area: Path, mapping: String)(implicit settings: Settings): Path = {
    if (mapping.contains("/"))
      new Path(mapping)
    else {
      val mappingFilename =
        if (mapping.endsWith(".yml")) mapping else mapping + ".sl.yml"
      findFileOrFallback(area, mappingFilename)
    }
  }

  protected def openAPIPath(openAPIFileName: String)(implicit settings: Settings): Path = {
    findFileOrFallback(new Path(DatasetArea.extract, "openapi"), openAPIFileName)
  }

  protected def schemaOutputDir(outputDir: Option[String])(implicit settings: Settings): Path =
    new Path(outputDir.getOrElse(DatasetArea.load.toString))

  protected def dataOutputDir(outputDir: Option[String])(implicit settings: Settings): Path =
    new Path(outputDir.getOrElse(DatasetArea.extract.toString))

  private def findFileOrFallback(area: Path, mappingFilename: String)(implicit
    settings: Settings
  ): Path = {
    val filePath = new Path(mappingFilename)
    val paths =
      settings
        .storageHandler()
        .list(area, recursive = false)
        .map(_.path)

    paths.find(_.getName() == mappingFilename).getOrElse(filePath)
  }
}
