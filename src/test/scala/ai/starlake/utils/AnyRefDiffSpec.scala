package ai.starlake.utils

import ai.starlake.TestHelper
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._

import java.io.InputStream

class AnyRefDiffSpec extends TestHelper {
  def readDomain(resource: String): Domain = {
    val lines: String = readYmlFile(resource)
    YamlSerializer.deserializeDomain(lines, "DOMAIN1OR2").get
  }

  def readJob(resource: String): AutoJobDesc = {
    val lines: String = readYmlFile(resource)
    YamlSerializer.deserializeJob(lines, "JOB1OR2").get
  }

  private def readYmlFile(resource: String) = {
    val stream: InputStream =
      getClass.getResourceAsStream(resource)
    val lines =
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    lines
  }

  new WithSettings() {
    val schemaHandler = new SchemaHandler(storageHandler)

    "Generic Diff" should "return all attributes that changed value" in {
      val attr1 = Attribute(
        "attr1",
        "invalid-type", // should raise error non existent type
        Some(true),
        required = true,
        PrivacyLevel(
          "MD5",
          false
        )
      )
      val attr2 = Attribute(
        "attr2",
        "invalid-type", // should raise error non existent type
        Some(true),
        required = true,
        PrivacyLevel(
          "MDX5",
          false
        ) // Should raise an error. Privacy cannot be applied on types other than string
      )

      val diff = AnyRefDiff.diffAny("ignore", attr1, attr2)
      println(diff)
    }

    "Generic Schema Diff" should "be valid" in {
      val domain1 = readDomain("/sample/diff/DOMAIN1.comet.yml")
      val domain2 = readDomain("/sample/diff/DOMAIN2.comet.yml")

      val res = Schema.compare(
        domain1.tables.find(_.name == "User").get,
        domain2.tables.find(_.name == "User").get
      )
      assert(res.isSuccess)
    }
  }
  "Generic Domain Diff" should "be valid" in {

    val domain1 = readDomain("/sample/diff/DOMAIN1.comet.yml")
    val domain2 = readDomain("/sample/diff/DOMAIN2.comet.yml")

    val res = Domain.compare(domain1, domain2)
    assert(res.isSuccess)
  }
  "Generic Job Diff" should "be valid" in {

    val job1 = readJob("/sample/diff/JOB1.comet.yml")
    val job2 = readJob("/sample/diff/JOB2.comet.yml")

    val res = AutoJobDesc.compare(job1, job2)
    println(res)
  }
}
