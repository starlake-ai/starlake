package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.schema.model.{
  DagDomain,
  DagGenerationConfig,
  DagGenerationContext,
  DagPair,
  DagSchedule
}
import ai.starlake.utils.Utils

import scala.jdk.CollectionConverters.{mapAsScalaMapConverter, seqAsJavaListConverter}

class Yml2DagGenerateCommandSpec extends TestHelper {
  new WithSettings() {
    "Parse Jinja" should "should be able to extract template file and access all variables" in {
      val templateContent = Yml2DagTemplateLoader.loadTemplate("sample.py.j2")
      val context = DagGenerationContext(
        config = DagGenerationConfig(
          template = "sample.py.j2",
          comment = "This is a comment",
          filename = "sample.py",
          options = Map("SL_OPTION1" -> "value1", "CUSTOM_OPTION2" -> "value2")
        ),
        schedules = List(
          DagSchedule(
            "0 0 * * *",
            List(
              DagDomain("domain1", List("table1", "table2").asJava),
              DagDomain("domain2", List("table3", "table4").asJava)
            ).asJava
          )
        )
      )
      val jContext = context.asMap

      val env = System.getenv().asScala
      val jEnv = env
        .map { case (k, v) =>
          DagPair(k, v)
        }
        .toList
        .asJava

      val result = Utils.parseJinjaTpl(
        templateContent,
        Map(
          "context" -> jContext,
          "env"     -> jEnv
        )
      )
      println(result)
    }
  }
}
