package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.core.utils.CaseClassToPojoConverter
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.jdk.CollectionConverters._

class Yml2DagGenerateCommandSpec extends TestHelper {
  new WithSettings() {
    "Parse Jinja" should "should be able to extract template file and access all variables" in {
      val templateContent = new Yml2DagTemplateLoader().loadTemplate("sample.py.j2")
      val context = LoadDagGenerationContext(
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
            ).asJava,
            List(
              CaseClassToPojoConverter.asJava(
                Domain(
                  name = "domain1",
                  rename = Some("finalDomain1"),
                  tables = List(
                    Schema(
                      name = "table1",
                      rename = Some("finalTable1"),
                      pattern = Pattern.compile("table1.*\\.json"),
                      attributes =
                        List(Attribute(name = "attr1", `type` = "int", comment = Some("Comment1")))
                    ),
                    Schema(
                      name = "table2",
                      rename = Some("finalTable2"),
                      pattern = Pattern.compile("table2.*\\.json"),
                      attributes = List(Attribute(name = "attr2", `type` = "double"))
                    )
                  )
                )
              ),
              CaseClassToPojoConverter.asJava(
                Domain(
                  name = "domain2",
                  rename = Some("finalDomain2"),
                  tables = List(
                    Schema(
                      name = "table3",
                      rename = Some("finalTable3"),
                      pattern = Pattern.compile("table3.*\\.json"),
                      attributes = List(
                        Attribute(name = "attr3", `type` = "variant", comment = Some("Comment3"))
                      )
                    ),
                    Schema(
                      name = "table4",
                      rename = Some("finalTable4"),
                      pattern = Pattern.compile("table4.*\\.json"),
                      attributes = List(Attribute(name = "attr4", `type` = "float"))
                    )
                  )
                )
              )
            ).asJava
          )
        ),
        workflowStatementsIn = List.empty[Map[String, Object]]
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
      dagContent should include("'rename':'finalDomain2'")
      dagContent should include("'pattern':'table3.*\\.json'")
      dagContent should include("'comment':'Comment3'")
      dagContent should include("'comment':''")
      println(dagContent)

    }

    "dag generation" should "should produce expected file" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/position/position.sl.yml",
        datasetDomainName = "position",
        sourceDatasetPathName = "/sample/position/XPOSTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "position",
          "/sample/position/account_position.sl.yml",
          Some("account.sl.yml")
        )
        val schemaHandler = settings.schemaHandler()
        new DagGenerateJob(schemaHandler).run(Array.empty)
        val dagPath = new Path(new Path(DatasetArea.build, "dags"), "position.py")
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
