package ai.starlake.tests

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.generator.AnyTemplateLoader
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

class StarlakeTestTemplateLoader extends AnyTemplateLoader with LazyLogging {
  protected def RESOURCE_TEMPLATE_FOLDER: String = s"templates/tests"
  protected def EXTERNAL_TEMPLATE_BASE_PATH(implicit settings: Settings): Path = DatasetArea.tests
}
