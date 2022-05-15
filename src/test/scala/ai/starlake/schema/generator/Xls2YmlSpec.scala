package ai.starlake.schema.generator

import better.files.File
import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.model.{BigQuerySink, Domain, Format}
import ai.starlake.utils.YamlSerializer

class Xls2YmlSpec extends TestHelper {
  new WithSettings {
    Xls2Yml.generateSchema(getClass.getResource("/sample/SomeDomainTemplate.xls").getPath)
    val outputFile = File(DatasetArea.domains.toString + "/someDomain.comet.yml")

    val result: Domain = YamlSerializer
      .deserializeDomain(outputFile)
      .getOrElse(throw new Exception(s"Invalid file name $outputFile"))

    "Parsing a sample xlsx file" should "generate a yml file" in {
      outputFile.exists() shouldBe true
      result.name shouldBe "someDomain"
      result.tables.size shouldBe 2
    }

    it should "trim leading of trailing spaces in cells contents" in {
      result.tables.map(_.name) should contain(
        "SCHEMA1"
      ) // while it is "SCHEMA1 " in the excel file
    }

    it should "take into account the index col of a schema" in {
      val sink = for {
        schema   <- result.tables.find(_.name == "SCHEMA1")
        metadata <- schema.metadata
        sink     <- metadata.sink
      } yield sink

      sink shouldBe Some(BigQuerySink())
    }

    "All configured schemas" should "have all declared attributes correctly set" in {
      val schema1 = result.tables.filter(_.name == "SCHEMA1").head
      schema1.metadata.flatMap(_.format) shouldBe Some(Format.POSITION)
      schema1.metadata.flatMap(_.encoding) shouldBe Some("UTF-8")
      schema1.attributes.size shouldBe 19
      schema1.merge.flatMap(_.timestamp) shouldBe Some("ATTRIBUTE_1")
      schema1.merge.map(_.key) shouldBe Some(List("ID1", "ID2"))

      val s1MaybePartitions = schema1.metadata.flatMap(_.partition)
      s1MaybePartitions
        .flatMap(_.attributes)
        .get
        .sorted shouldEqual List("comet_day", "comet_hour", "comet_month", "comet_year")
      s1MaybePartitions
        .flatMap(_.sampling)
        .get shouldEqual 10.0

      val schema2 = result.tables.filter(_.name == "SCHEMA2").head
      schema2.metadata.flatMap(_.format) shouldBe Some(Format.DSV)
      schema2.metadata.flatMap(_.encoding) shouldBe Some("ISO-8859-1")
      schema2.attributes.size shouldBe 19

      val s2MaybePartitions = schema2.metadata.flatMap(_.partition)
      s2MaybePartitions
        .flatMap(_.attributes)
        .get
        .sorted shouldEqual List("RENAME_ATTRIBUTE_8", "RENAME_ATTRIBUTE_9")
      s2MaybePartitions
        .flatMap(_.sampling)
        .get shouldEqual 0.0
    }

    val reader = new XlsReader(Path(getClass.getResource("/sample/SomeDomainTemplate.xls").getPath))
    val domainOpt = reader.getDomain()

    "a preEncryption domain" should "have only string types" in {
      domainOpt shouldBe defined
      val preEncrypt = Xls2Yml.genPreEncryptionDomain(domainOpt.get, Nil)
      preEncrypt.tables.flatMap(_.attributes).filter(_.`type` != "string") shouldBe empty
    }

    "Merge and Partition elements" should "only be present in Post-Encryption domain" in {
      domainOpt shouldBe defined
      val preEncrypt = Xls2Yml.genPreEncryptionDomain(domainOpt.get, Nil)
      preEncrypt.tables.flatMap(_.metadata.map(_.partition)).forall(p => p.isEmpty) shouldBe true
      preEncrypt.tables.map(_.merge).forall(m => m.isEmpty) shouldBe true
      val postEncrypt = Xls2Yml.genPostEncryptionDomain(domainOpt.get, None, Nil)
      postEncrypt.tables
        .flatMap(_.metadata.map(_.partition))
        .forall(p => p.isDefined) shouldBe true
      postEncrypt.tables.map(_.merge).forall(m => m.isDefined) shouldBe true

    }

    "Column Description in schema" should "be present" in {
      domainOpt shouldBe defined
      domainOpt.get.tables.flatMap(_.comment) should have length 1
    }

    private def validCount(domain: Domain, algo: String, count: Int) =
      domain.tables
        .flatMap(_.attributes)
        .filter(_.getPrivacy().toString == algo) should have length count

    "SHA1 & HIDE privacy policies" should "be applied in the pre-encrypt step " in {
      domainOpt shouldBe defined
      val preEncrypt = Xls2Yml.genPreEncryptionDomain(domainOpt.get, List("HIDE", "SHA1"))
      validCount(preEncrypt, "HIDE", 2)
      validCount(preEncrypt, "MD5", 0)
      validCount(preEncrypt, "SHA1", 1)
    }
    "All privacy policies" should "be applied in the pre-encrypt step " in {
      domainOpt shouldBe defined
      val preEncrypt = Xls2Yml.genPreEncryptionDomain(domainOpt.get, Nil)
      validCount(preEncrypt, "HIDE", 2)
      validCount(preEncrypt, "MD5", 2)
      validCount(preEncrypt, "SHA1", 1)
    }
    "In prestep Attributes" should "not be renamed" in {
      domainOpt shouldBe defined
      val preEncrypt = Xls2Yml.genPreEncryptionDomain(domainOpt.get, Nil)
      val schemaOpt = preEncrypt.tables.find(_.name == "SCHEMA1")
      schemaOpt shouldBe defined
      val attrOpt = schemaOpt.get.attributes.find(_.name == "ATTRIBUTE_6")
      attrOpt shouldBe defined
      attrOpt.get.rename shouldBe None
    }

    "In poststep Attributes" should "keep renaming strategy" in {
      domainOpt shouldBe defined
      val postEncrypt = Xls2Yml.genPostEncryptionDomain(domainOpt.get, Some("µ"), Nil)
      val schemaOpt = postEncrypt.tables.find(_.name == "SCHEMA1")
      schemaOpt shouldBe defined
      val attrOpt = schemaOpt.get.attributes.find(_.name == "ATTRIBUTE_6")
      attrOpt shouldBe defined
      attrOpt.get.rename shouldBe defined
      attrOpt.get.rename.get shouldBe "RENAME_ATTRIBUTE_6"

    }
    "No privacy policies" should "be applied in the post-encrypt step " in {
      domainOpt shouldBe defined
      val postEncrypt = Xls2Yml.genPostEncryptionDomain(domainOpt.get, Some("µ"), Nil)
      validCount(postEncrypt, "HIDE", 0)
      validCount(postEncrypt, "MD5", 0)
      validCount(postEncrypt, "SHA1", 0)
    }

    "a preEncryption domain" should " not have required attributes" in {
      domainOpt shouldBe defined
      val preEncrypt = Xls2Yml.genPreEncryptionDomain(domainOpt.get, Nil)
      preEncrypt.tables.flatMap(_.attributes).filter(_.required) shouldBe empty
    }

    "a postEncryption domain" should "have not have POSITION schemas" in {
      domainOpt shouldBe defined
      domainOpt.get.tables
        .flatMap(_.metadata)
        .count(_.format.contains(Format.POSITION)) shouldBe 1
      val postEncrypt =
        Xls2Yml.genPostEncryptionDomain(domainOpt.get, Some("µ"), List("HIDE", "SHA1"))
      postEncrypt.tables
        .flatMap(_.metadata)
        .filter(_.format.contains(Format.POSITION)) shouldBe empty
      validCount(postEncrypt, "HIDE", 0)
      validCount(postEncrypt, "MD5", 2)
      validCount(postEncrypt, "SHA1", 0)
    }
    "a custom separator" should "be generated" in {
      domainOpt shouldBe defined
      domainOpt.get.tables
        .flatMap(_.metadata)
        .count(_.format.contains(Format.POSITION)) shouldBe 1
      val postEncrypt = Xls2Yml.genPostEncryptionDomain(domainOpt.get, Some(","), Nil)
      postEncrypt.tables
        .flatMap(_.metadata)
        .filterNot(_.separator.contains(",")) shouldBe empty
    }

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
          |  --files <value>       List of Excel files describing Domains & Schemas
          |  --encryption <value>  If true generate pre and post encryption YML
          |  --delimiter <value>   CSV delimiter to use in post-encrypt YML.
          |  --privacy <value>     What privacy policies should be applied in the pre-encryption phase ?
          | All privacy policies are applied by default.
          |  --outputPath <value>  Path for saving the resulting YAML file(s).
          | Comet domains path is used by default.
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }

}
