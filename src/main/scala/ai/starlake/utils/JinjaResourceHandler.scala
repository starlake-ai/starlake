package ai.starlake.utils

import ai.starlake.config.Settings
import com.hubspot.jinjava.interpret.JinjavaInterpreter
import com.hubspot.jinjava.loader.ResourceLocator
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset
import scala.tools.nsc.io.File

class JinjaResourceHandler(implicit settings: Settings) extends ResourceLocator {
  override def getString(
    fullName: String,
    encoding: Charset,
    interpreter: JinjavaInterpreter
  ): String = {
    val path =
      if (fullName.startsWith(File.separator) || fullName.contains(":"))
        new Path(fullName)
      else
        new Path(settings.comet.metadata, fullName)
    settings.metadataStorageHandler.read(path)
  }
}
