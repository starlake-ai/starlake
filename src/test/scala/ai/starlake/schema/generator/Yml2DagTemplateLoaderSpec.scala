package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import org.apache.hadoop.fs.Path

import scala.jdk.CollectionConverters._

class Yml2DagTemplateLoaderSpec extends TestHelper {
  new WithSettings() {
    "Parse Jinja" should "should be able to extract template file and access all variables" in {
      val templates = new Yml2DagTemplateLoader().allLoadTemplates()
      for (elem <- templates) { println(elem) }
    }
  }
}
