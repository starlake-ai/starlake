package com.ebiznext.comet.schema.generator

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.schema.handlers.SchemaHandler

class Yml2XlsSpec extends TestHelper {
  "Yml2XLS" should "should generated all domain / schema in XLS files" in {
    new WithSettings() {
      new SpecTrait(
        domainFilename = "position.comet.yml",
        sourceDomainPathname = "/sample/position/position.comet.yml",
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
          assert(domain.schemas.size == 1)
          assert(domain.schemas.head.name == "account")
          domain.schemas.head.attributes.size == 10
        }
      }
    }
  }

  "All SchemaGen Config" should "be known and taken  into account" in {
    val rendered = Yml2XlsConfig.usage()
    val expected =
      """
        |Usage: comet yml2xls [options]
        |
        |  --domain <value>  domains to convert to XLS
        |  --xls <value>     directory where XLS files are generated
        |""".stripMargin
    rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
      .replaceAll("\\s", "")

  }

}
