package com.ebiznext.comet.schema.generator

import java.io.File

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.schema.model.{Domain, Format, PrivacyLevel}

class SchemaGenSpec extends TestHelper {
  new WithSettings() {
    "Parsing a sample xlsx file" should "generate a yml file" in {
      SchemaGen.generateSchema(getClass().getResource("/sample/SomeDomainTemplate.xls").getPath)
      val outputFile = new File(DatasetArea.domains.toString + "/someDomain.yml")
      outputFile.exists() shouldBe true
      val result = YamlSerializer.mapper.readValue(outputFile, classOf[Domain])
      result.name shouldBe "someDomain"
      result.schemas.size shouldBe 2
      val schema1 = result.schemas.filter(_.name == "SCHEMA1").head
      schema1.metadata.flatMap(_.format) shouldBe Some(Format.POSITION)
      schema1.attributes.size shouldBe 19
      schema1.merge.flatMap(_.timestamp) shouldBe Some("ATTRIBUTE_1")
      schema1.merge.map(_.key) shouldBe Some(List("ID1", "ID2"))
      schema1.metadata.flatMap(_.encoding) shouldBe Some("UTF-8")
      val schema2 = result.schemas.filter(_.name == "SCHEMA2").head
      schema2.metadata.flatMap(_.format) shouldBe Some(Format.DSV)
      schema2.metadata.flatMap(_.encoding) shouldBe Some("ISO-8859-1")
      schema2.attributes.size shouldBe 19
    }

    val reader = new XlsReader(getClass().getResource("/sample/SomeDomainTemplate.xls").getPath)
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
    "No privacy policies" should "be applied in the post-encrypt step " in {
      domainOpt shouldBe defined
      val preEncrypt = SchemaGen.genPostEncryptionDomain(domainOpt.get, "µ", Nil)
      validCount(preEncrypt, "HIDE", 0)
      validCount(preEncrypt, "MD5", 0)
      validCount(preEncrypt, "SHA1", 0)
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
      val postEncrypt = SchemaGen.genPostEncryptionDomain(domainOpt.get, "µ", List("HIDE", "SHA1"))
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
      val postEncrypt = SchemaGen.genPostEncryptionDomain(domainOpt.get, ",", Nil)
      postEncrypt.schemas
        .flatMap(_.metadata)
        .filterNot(_.separator.contains(",")) shouldBe empty
    }
  }

}
