package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

class Yml2DagTemplateLoader extends AnyTemplateLoader with LazyLogging {
  protected def RESOURCE_TEMPLATE_FOLDER: String = s"templates/dags"
  protected def EXTERNAL_TEMPLATE_BASE_PATH(implicit settings: Settings): Path = DatasetArea.dags
}
