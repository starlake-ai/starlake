package ai.starlake.utils

import ai.starlake.TestHelper
import ai.starlake.schema.{ProjectCompare, ProjectCompareConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import org.apache.hadoop.fs.Path

import java.io.InputStream

// TODO check tests return values and support folder in tests
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

      val diff = AnyRefDiff.diffAnyRef("ignore", attr1, attr2)
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
    "Generic Domain Diff" should "be valid" in {

      val domain1 = readDomain("/sample/diff/DOMAIN1.comet.yml")
      val domain2 = readDomain("/sample/diff/DOMAIN2.comet.yml")

      val res = Domain.compare(domain1, domain2)
      assert(res.isSuccess)
    }
    "Generic Project Diff" should "be valid" in {
      val resource1 = getClass.getClassLoader.getResource(
        "/Users/hayssams/git/public/starlake/internal/anyref/quickstart1"
      )
      val resource2 = getClass.getClassLoader.getResource(
        "/Users/hayssams/git/public/starlake/internal/anyref/quickstart2"
      )
      ProjectCompare.compare(
        ProjectCompareConfig(
          "/Users/hayssams/git/public/starlake/internal/anyref/quickstart1",
          "/Users/hayssams/git/public/starlake/internal/anyref/quickstart2"
        )
      )
      val res = Project.compare(
        new Path("/Users/hayssams/git/public/starlake/internal/anyref/quickstart1"),
        new Path("/Users/hayssams/git/public/starlake/internal/anyref/quickstart2")
        // new Path(resource1.toURI),
        // new Path(resource2.toURI)
      )(settings)
      println(JsonSerializer.serializeObject(res))
    }
    "Generic Job Diff" should "be valid" in {

      val job1 = readJob("/sample/diff/JOB1.comet.yml")
      val job2 = readJob("/sample/diff/JOB2.comet.yml")

      val res = AutoJobDesc.compare(job1, job2)
      println(res)
    }
  }
  "ACE Diff" should "be valid" in {
    val ace1 = AccessControlEntry("role1", List("user1", "user2", "user3"))
    val ace2 = AccessControlEntry("role1", List("user1", "user4", "user3"))
    val res = ace1.compare(ace2)
    val expected = ListDiff(
      "role1",
      Nil,
      Nil,
      List(
        (
          NamedValue("grants", List("user1", "user2", "user3")),
          NamedValue("grants", List("user1", "user4", "user3"))
        )
      )
    )
    assert(res == expected)
  }

  "MergeOptions Diff" should "be valid" in {
    val m1 = MergeOptions(List("key1", "key2"), None, Some("ts1"))
    val m2 = MergeOptions(List("key1", "key2"), None, Some("ts2"))
    val res = m1.compare(m2)
    val expected = ListDiff(
      "",
      Nil,
      Nil,
      List(
        (
          NamedValue("timestamp", Some("ts1")),
          NamedValue("timestamp", Some("ts2"))
        )
      )
    )
    println(res)
    assert(res == expected)
  }

  "RowLevelSecurity Diff" should "be valid" in {
    val rls1 =
      RowLevelSecurity("rls", predicate = "false", grants = Set("gr1"), description = "desc1")
    val rls2 =
      RowLevelSecurity("rls", predicate = "false", grants = Set("gr1"), description = "desc2")
    val res = rls1.compare(rls2)
    val expected = ListDiff(
      "rls",
      Nil,
      Nil,
      List((NamedValue("description", "desc1"), NamedValue("description", "desc2")))
    )
    assert(res == expected)
  }
  "Metadata Diff" should "be valid" in {
    val metadata1 = Metadata(
      mode = Some(Mode.STREAM),
      quote = Some("::"),
      clustering = Some(List("col1", "col2")),
      directory = Some("/{{domain}}/{{schema}}")
    )
    val metadata2 = Metadata(
      mode = Some(Mode.FILE),
      quote = Some("::"),
      clustering = Some(List("col1", "col2")),
      directory = Some("/{{domain}}/{{schema}}")
    )

    val res = metadata1.compare(metadata2)
    val expected = ListDiff(
      "",
      Nil,
      Nil,
      List((NamedValue("mode", Some(Mode.STREAM)), NamedValue("mode", Some(Mode.FILE))))
    )
    assert(res == expected)
  }
}
