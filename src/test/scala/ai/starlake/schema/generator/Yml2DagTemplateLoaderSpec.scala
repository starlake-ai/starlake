package ai.starlake.schema.generator

import ai.starlake.TestHelper

class Yml2DagTemplateLoaderSpec extends TestHelper {
  // dag templates have been moved to their own starlake-orchestration project
  new WithSettings() {
    "Parse Jinja" should "should be able to extract template file and access all variables" ignore {
      val templates = new Yml2DagTemplateLoader().allLoadTemplates()
      for (elem <- templates) { println(elem) }
    }
  }
}
