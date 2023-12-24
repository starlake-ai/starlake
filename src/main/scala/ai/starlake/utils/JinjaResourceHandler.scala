package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.schema.generator.Yml2DagTemplateLoader.RESOURCE_DOMAIN_TEMPLATE_FOLDER
import better.files.Resource
import com.hubspot.jinjava.interpret.JinjavaInterpreter
import com.hubspot.jinjava.loader.ResourceLocator
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset
import scala.tools.nsc.io.File
import scala.util.{Failure, Success, Try}

class JinjaResourceHandler(implicit settings: Settings) extends ResourceLocator {

  private def isAbsolute(path: String): Boolean = {
    // linux of windows absolute path
    path.startsWith(File.separator) || path.contains(":")
  }

  override def getString(
    fullName: String,
    encoding: Charset,
    interpreter: JinjavaInterpreter
  ): String = {
    val path =
      if (isAbsolute(fullName))
        new Path(fullName)
      else
        new Path(settings.appConfig.metadata, fullName)
    if (settings.storageHandler().exists(path))
      settings.storageHandler().read(path)
    else
      Resource.asString(fullName) match {
        case Some(value) => value
        case None =>
          throw new RuntimeException(s"Relative template not found in for ${fullName}")
      }
  }
}
