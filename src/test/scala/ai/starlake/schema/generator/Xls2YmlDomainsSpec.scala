package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.model.{BigQuerySink, Domain, Format, Schema}
import ai.starlake.utils.YamlSerializer
import better.files.File

import scala.util.{Failure, Success}

class Xls2YmlDomainsSpec extends TestHelper {
  new WithSettings() {
    Xls2Yml.writeDomainsAsYaml(
      File(getClass.getResource("/sample/SomeDomainTemplate.xls")).pathAsString
    )
    val outputPath = File(DatasetArea.load.toString + "/someDomain/_config.sl.yml")
    val schema1Path = File(DatasetArea.load.toString + "/someDomain/SCHEMA1.sl.yml")
    val schema2Path = File(DatasetArea.load.toString + "/someDomain/SCHEMA2.sl.yml")

    val result: Domain = YamlSerializer
      .deserializeDomain(outputPath.contentAsString, outputPath.pathAsString) match {
      case Success(value)     => value
      case Failure(exception) => throw exception
    }

    val schema1: Schema = YamlSerializer
      .deserializeSchemaRefs(schema1Path.contentAsString, schema1Path.pathAsString)
      .tables
      .head

    val schema2: Schema = YamlSerializer
      .deserializeSchemaRefs(schema2Path.contentAsString, schema2Path.pathAsString)
      .tables
      .head

    "Parsing a sample xlsx file" should "generate a yml file" in {
      outputPath.exists shouldBe true
      result.name shouldBe "someDomain"
    }

    it should "take into account the index col of a schema" in {
      val sink = for {
        schema   <- Some(schema1)
        metadata <- schema.metadata
        sink     <- metadata.sink
      } yield sink

      sink.map(_.getSink()) shouldBe Some(
        BigQuerySink(Some("BQ"), None, None, None, None, None, None, None)
      )
    }

    "All configured schemas" should "have all declared attributes correctly set" in {
      schema1.metadata.flatMap(_.format) shouldBe Some(Format.POSITION)
      schema1.metadata.flatMap(_.encoding) shouldBe Some("UTF-8")
      schema1.attributes.size shouldBe 19
      schema1.merge.flatMap(_.timestamp) shouldBe Some("ATTRIBUTE_1")
      schema1.merge.map(_.key) shouldBe Some(List("ID1", "ID2"))

      val s1MaybePartitions = schema1.metadata.flatMap(_.partition)
      s1MaybePartitions
        .map(_.attributes)
        .get
        .sorted shouldEqual List("comet_day", "comet_hour", "comet_month", "comet_year")
      s1MaybePartitions
        .flatMap(_.sampling)
        .get shouldEqual 10.0

      schema2.metadata.flatMap(_.format) shouldBe Some(Format.DSV)
      schema2.metadata.flatMap(_.encoding) shouldBe Some("ISO-8859-1")
      schema2.attributes.size shouldBe 19

      val s2MaybePartitions = schema2.metadata.flatMap(_.partition)
      s2MaybePartitions
        .map(_.attributes)
        .get
        .sorted shouldEqual List("RENAME_ATTRIBUTE_8", "RENAME_ATTRIBUTE_9")
      s2MaybePartitions
        .flatMap(_.sampling)
        .get shouldEqual 0.0
    }

    val reader = new XlsDomainReader(
      InputPath(getClass.getResource("/sample/SomeDomainTemplate.xls").getPath)
    )
    val domainOpt = reader.getDomain()

    "a complex XLS (aka JSON/XML)" should "produce the correct schema" in {
      val complexReader =
        new XlsDomainReader(
          InputPath(
            File(getClass.getResource("/sample/SomeComplexDomainTemplate.xls")).pathAsString
          )
        )
      val xlsTable = complexReader.getDomain().get.tables.head
      val yamlPath =
        File(getClass.getResource("/sample/SomeComplexDomainTemplate.sl.yml"))

      val yamlTable = YamlSerializer
        .deserializeDomain(yamlPath.contentAsString, yamlPath.pathAsString)
        .getOrElse(throw new Exception(s"Invalid file name $yamlPath"))
        .tables
        .head

      xlsTable.attributes.length shouldBe yamlTable.attributes.length

      deepEquals(xlsTable.attributes, yamlTable.attributes)
    }

    "Column Description in schema" should "be present" in {
      domainOpt shouldBe defined
      domainOpt.get.tables.flatMap(_.comment) should have length 1
    }

    private def validCount(domain: Domain, algo: String, count: Int) =
      domain.tables
        .flatMap(_.attributes)
        .filter(_.getPrivacy().toString == algo) should have length count

    "a scripted attribute" should "be generated" in {
      domainOpt shouldBe defined
      domainOpt
        .flatMap(_.tables.find(_.name == "SCHEMA1"))
        .flatMap(_.attributes.find(_.name == "ATTRIBUTE_4").flatMap(_.script)) shouldBe Some(
        "current_date()"
      )
    }

    "All SchemaGen Config" should "be known and taken  into account" in {
      val rendered = Xls2YmlConfig.usage()
      val expected =
        """
          |Usage: starlake xls2yml [options]
          |
          |  --files <value>       List of Excel files describing domains & schemas or jobs
          |  --iamPolicyTagsFile <value>
          |                        If true generate IAM PolicyTags YML
          |  --outputPath <value>  Path for saving the resulting YAML file(s).
          | Starlake domains path is used by default.
          |  --policyFile <value>  Optional File for centralising ACL & RLS definition.
          |  --job <value>         If true generate YML for a Job.
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }

}
