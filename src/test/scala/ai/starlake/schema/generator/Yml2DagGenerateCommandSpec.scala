package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import org.apache.hadoop.fs.Path

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
            "0 0 * * *",
            List(
              DagDomain(
                "domain1",
                "finalDomain1",
                List(
                  TableDomain("table1", "finalTable1"),
                  TableDomain("table2", "finalTable2")
                ).asJava
              ),
              DagDomain(
                "domain2",
                "finalDomain2",
                List(
                  TableDomain("table3", "finalTable3"),
                  TableDomain("table4", "finalTable4")
                ).asJava
              )
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

      val dagContent = Utils.parseJinjaTpl(
        templateContent,
        Map(
          "context" -> jContext,
          "env"     -> jEnv
        )
      )

      dagContent should include("description='This is a comment'")
      dagContent should include("SL_OPTION1':'value1'")
      dagContent should include("name':'domain1'")
      dagContent should include("table2'")
      println(dagContent)

    }

    "dag generation" should "should produce expected file" in {
      new SpecTrait(
        domainOrJobFilename = "position.sl.yml",
        sourceDomainOrJobPathname = "/sample/position/position.sl.yml",
        datasetDomainName = "position",
        sourceDatasetPathName = "/sample/position/XPOSTBL"
      ) {
        cleanMetadata
        cleanDatasets
        val schemaHandler = new SchemaHandler(settings.storageHandler())
        new Yml2DagGenerateCommand(schemaHandler).run(Array.empty)
        val dagPath = new Path(new Path(DatasetArea.dags, "generated/load"), "position.py")
        settings.storageHandler().exists(dagPath) shouldBe true
        val dagContent = settings.storageHandler().read(dagPath)
        dagContent should include("description='sample dag configuration'")
        dagContent should include("'profileVar':'DATAPROC_MEDIUM'")
        dagContent should include("'name':'position'")
        dagContent should include("'final_name':'position'")
      }
    }
  }
}
