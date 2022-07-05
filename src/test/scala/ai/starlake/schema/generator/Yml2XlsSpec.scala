package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.YamlSerializer
import better.files.File

class Yml2XlsSpec extends TestHelper {
  "Yml2XLS" should "should generated all domain / schema in XLS files" in {
    new WithSettings() {
      new SpecTrait(
        domainOrJobFilename = "position.comet.yml",
        sourceDomainOrJobPathname = "/sample/position/position.comet.yml",
        datasetDomainName = "position",
        sourceDatasetPathName = "/sample/position/XPOSTBL"
      ) {
        cleanMetadata
        cleanDatasets
        val schemaHandler = new SchemaHandler(settings.storageHandler)
        new Yml2XlsWriter(schemaHandler).generateXls(Nil, "/tmp")
        val reader = new XlsReader(Path("/tmp/position.xlsx"))
        val domain = reader.getDomain()
        assert(domain.isDefined)
        domain.foreach { domain =>
          assert(domain.name == "position")
          assert(domain.tables.size == 2)
          val accountSchema = domain.tables.filter(_.name == "account")
          assert(accountSchema.size == 1)
          accountSchema.head.attributes.size == 10
        }
      }
    }
  }

  new WithSettings() {
    new SpecTrait(
      domainOrJobFilename = "position.comet.yml",
      sourceDomainOrJobPathname = "/sample/position/position.comet.yml",
      datasetDomainName = "position",
      sourceDatasetPathName = "/sample/position/XPOSTBL"
    ) {
      "a complex attribute list(aka JSON/XML)" should "produce the correct XLS file" in {
        val yamlFile =
          File(getClass.getResource("/sample/SomeComplexDomainTemplate.comet.yml").getPath)
        val yamlDomain = YamlSerializer
          .deserializeDomain(yamlFile)
          .getOrElse(throw new Exception(s"Invalid file name $yamlFile"))
        val schemaHandler = new SchemaHandler(settings.storageHandler)
        new Yml2XlsWriter(schemaHandler).writeDomainXls(yamlDomain, "/tmp")
        val xlsOut = File("/tmp", yamlDomain.name + ".xlsx")
        val complexReader =
          new XlsReader(Path(xlsOut.pathAsString))
        val xlsTable = complexReader.getDomain().get.tables.head
        val yamlTable = yamlDomain.tables.head
        xlsTable.attributes.length shouldBe yamlTable.attributes.length

        deepEquals(xlsTable.attributes, yamlTable.attributes)
      }
    }
  }

  "All SchemaGen Config" should "be known and taken  into account" in {
    val rendered = Yml2XlsConfig.usage()
    val expected =
      """
        |Usage: starlake yml2xls [options]
        |
        |  --domain <value>  domains to convert to XLS
        |  --xls <value>     directory where XLS files are generated
        |""".stripMargin
    rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
      .replaceAll("\\s", "")

  }

}
