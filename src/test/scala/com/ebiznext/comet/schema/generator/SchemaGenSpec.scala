package com.ebiznext.comet.schema.generator

import java.io.File

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.schema.model.{Domain, Format, PrivacyLevel}

class SchemaGenSpec extends TestHelper {
  new WithSettings() {

    SchemaGen.generateSchema(getClass.getResource("/sample/SomeDomainTemplate.xls").getPath)
    val outputFile = new File(DatasetArea.domains.toString + "/someDomain.yml")
    val result: Domain = YamlSerializer.mapper.readValue(outputFile, classOf[Domain])

    "Parsing a sample xlsx file" should "generate a yml file" in {
      outputFile.exists() shouldBe true
      result.name shouldBe "someDomain"
      result.schemas.size shouldBe 2
    }

    it should "trim leading of trailing spaces in cells contents" in {
      result.schemas.map(_.name) should contain(
        "SCHEMA1"
      ) // while it is "SCHEMA1 " in the excel file
    }

    "All configured schemas" should "have all declared attributes correctly set" in {
      val schema1 = result.schemas.filter(_.name == "SCHEMA1").head
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

      val schema2 = result.schemas.filter(_.name == "SCHEMA2").head
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

    val reader = new XlsReader(getClass.getResource("/sample/SomeDomainTemplate.xls").getPath)
    val domainOpt = reader.getDomain()

    "a preEncryption domain" should "have only string types" in {
      domainOpt shouldBe defined
      val preEncrypt = SchemaGen.genPreEncryptionDomain(domainOpt.get, Nil)
      preEncrypt.schemas.flatMap(_.attributes).filter(_.`type` != "string") shouldBe empty
    }

    "Column Description in schema" should "be present" in {
      domainOpt shouldBe defined
      domainOpt.get.schemas.flatMap(_.comment) should have length 1
    }

    private def validCount(domain: Domain, algo: String, count: Int) =
      domain.schemas
        .flatMap(_.attributes)
        .filter(_.privacy.getOrElse(PrivacyLevel.None).toString == algo) should have length count

    "SHA1 & HIDE privacy policies" should "be applied in the pre-encrypt step " in {
      domainOpt shouldBe defined
      val preEncrypt = SchemaGen.genPreEncryptionDomain(domainOpt.get, List("HIDE", "SHA1"))
      validCount(preEncrypt, "HIDE", 2)
      validCount(preEncrypt, "MD5", 0)
      validCount(preEncrypt, "SHA1", 1)
    }
    "All privacy policies" should "be applied in the pre-encrypt step " in {
      domainOpt shouldBe defined
      val preEncrypt = SchemaGen.genPreEncryptionDomain(domainOpt.get, Nil)
      validCount(preEncrypt, "HIDE", 2)
      validCount(preEncrypt, "MD5", 2)
      validCount(preEncrypt, "SHA1", 1)
    }
    "In prestep Attributes" should "keep renaming strategy" in {
      domainOpt shouldBe defined
      val preEncrypt = SchemaGen.genPreEncryptionDomain(domainOpt.get, Nil)
      val schemaOpt = preEncrypt.schemas.find(_.name == "SCHEMA1")
      schemaOpt shouldBe defined
      val attrOpt = schemaOpt.get.attributes.find(_.name == "ATTRIBUTE_6")
      attrOpt shouldBe defined
      attrOpt.get.rename shouldBe defined
    }

    "In poststep Attributes" should "be already renamed" in {
      domainOpt shouldBe defined
      val postEncrypt = SchemaGen.genPostEncryptionDomain(domainOpt.get, Some("µ"), Nil)
      val schemaOpt = postEncrypt.schemas.find(_.name == "SCHEMA1")
      schemaOpt shouldBe defined
      val attrOpt = schemaOpt.get.attributes.find(_.name == "RENAME_ATTRIBUTE_6")
      attrOpt shouldBe defined
      attrOpt.get.rename shouldBe None

    }
    "No privacy policies" should "be applied in the post-encrypt step " in {
      domainOpt shouldBe defined
      val postEncrypt = SchemaGen.genPostEncryptionDomain(domainOpt.get, Some("µ"), Nil)
      validCount(postEncrypt, "HIDE", 0)
      validCount(postEncrypt, "MD5", 0)
      validCount(postEncrypt, "SHA1", 0)
    }

    "a preEncryption domain" should " not have required attributes" in {
      domainOpt shouldBe defined
      val preEncrypt = SchemaGen.genPreEncryptionDomain(domainOpt.get, Nil)
      preEncrypt.schemas.flatMap(_.attributes).filter(_.required) shouldBe empty
    }

    "a postEncryption domain" should "have not have POSITION schemas" in {
      domainOpt shouldBe defined
      domainOpt.get.schemas
        .flatMap(_.metadata)
        .count(_.format.contains(Format.POSITION)) shouldBe 1
      val postEncrypt =
        SchemaGen.genPostEncryptionDomain(domainOpt.get, Some("µ"), List("HIDE", "SHA1"))
      postEncrypt.schemas
        .flatMap(_.metadata)
        .filter(_.format.contains(Format.POSITION)) shouldBe empty
      validCount(postEncrypt, "HIDE", 0)
      validCount(postEncrypt, "MD5", 2)
      validCount(postEncrypt, "SHA1", 0)
    }
    "a custom separator" should "be generated" in {
      domainOpt shouldBe defined
      domainOpt.get.schemas
        .flatMap(_.metadata)
        .count(_.format.contains(Format.POSITION)) shouldBe 1
      val postEncrypt = SchemaGen.genPostEncryptionDomain(domainOpt.get, Some(","), Nil)
      postEncrypt.schemas
        .flatMap(_.metadata)
        .filterNot(_.separator.contains(",")) shouldBe empty
    }
    "All SchemaGen Config" should "be known and taken  into account" in {
      val rendered = SchemaGenConfig.usage()
      val expected =
        """
          |Usage: comet [options]
          |
          |  --files <value>       List of Excel files describing Domains & Schemas
          |  --encryption <value>  If true generate pre and post encryption YML
          |  --delimiter <value>   CSV delimiter to use in post-encrypt YML.
          |  --privacy <value>     What privacy policies should be applied in the pre-encryption phase ?
          | All privacy policies are applied by default.
          |  --outputPath <value>  Path for saving the resulting YAML file(s).
          | COMET domains path is used by default.
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }

}
