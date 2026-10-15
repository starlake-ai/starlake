package ai.starlake.serve.catalog

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success}

class CatalogJsonSpec extends AnyFlatSpec with Matchers {

  "CatalogJson.ok" should "wrap data and keep empty lists and null options" in {
    CatalogJson.ok(List.empty[CatalogSchema]) shouldBe """{"ok":true,"data":[]}"""
    CatalogJson.ok(List(CatalogTable("t", "TABLE", None))) shouldBe
    """{"ok":true,"data":[{"name":"t","kind":"TABLE","remarks":null}]}"""
  }

  it should "serialize preview rows with nulls" in {
    val preview = CatalogPreview(
      List(CatalogPreviewColumn("a", "INTEGER")),
      List(List(1L), List(null)),
      truncated = false
    )
    CatalogJson.ok(preview) shouldBe
    """{"ok":true,"data":{"columns":[{"name":"a","type":"INTEGER"}],"rows":[[1],[null]],"truncated":false}}"""
  }

  "CatalogJson.fromTry" should "map a CatalogException to its code" in {
    CatalogJson.fromTry(
      Failure(new CatalogException(CatalogErrorCode.UnknownConnection, "Unknown connection 'x'"))
    ) shouldBe """{"ok":false,"error":{"code":"UNKNOWN_CONNECTION","message":"Unknown connection 'x'"}}"""
  }

  it should "map any other exception to QUERY_FAILED" in {
    CatalogJson.fromTry(Failure(new RuntimeException("boom"))) shouldBe
    """{"ok":false,"error":{"code":"QUERY_FAILED","message":"boom"}}"""
  }

  it should "wrap a success" in {
    CatalogJson.fromTry(Success(List(CatalogSchema("main")))) shouldBe
    """{"ok":true,"data":[{"name":"main"}]}"""
  }

  "CatalogJson.ok" should "serialize a connection with active and ordered details" in {
    val details = scala.collection.immutable.ListMap("url" -> "jdbc:duckdb:/x.db")
    CatalogJson.ok(
      CatalogConnection("duck", "JDBC", "duckdb", browsable = true, None, active = true, details)
    ) shouldBe
    """{"ok":true,"data":{"name":"duck","type":"JDBC","engine":"duckdb","browsable":true,"reason":null,"active":true,"details":{"url":"jdbc:duckdb:/x.db"}}}"""
    CatalogJson.ok(
      CatalogConnection(
        "x",
        "FS",
        "unknown",
        browsable = false,
        Some("r"),
        active = false,
        scala.collection.immutable.ListMap.empty
      )
    ) should include(""""active":false,"details":{}""")
  }

  "CatalogScrubber.maskOptions" should "sort keys and mask secrets, url passwords and nulls" in {
    val masked = CatalogScrubber.maskOptions(
      Map(
        "url"      -> "jdbc:postgresql://h/db?password=abc",
        "password" -> "x",
        "user"     -> "bob",
        "note"     -> null
      )
    )
    masked.toList shouldBe List(
      "note"     -> "",
      "password" -> "***",
      "url"      -> "jdbc:postgresql://h/db?password=***",
      "user"     -> "bob"
    )
  }

  it should "mask cloud credential option names and URL userinfo in maskOptions" in {
    val options = Map(
      "jsonKeyBase64" -> "QUJDREVGRw==",
      "jsonKeyfile"   -> "/secret/path/key.json",
      "apiKey"        -> "AK-123456",
      "accessKey"     -> "ACC-123456",
      "privateKey"    -> "PK-123456",
      "pgUrl"         -> "jdbc:postgresql://u:S3cret@h:5432/db",
      "httpUrl"       -> "https://u:S3cret@h/x",
      "paramUrl"      -> "jdbc:x://h/db?api_key=ZZZ999&access_key=YYY888&sas_key=XXX777"
    )
    val masked = CatalogScrubber.maskOptions(options)
    masked.values.foreach { v =>
      List(
        "QUJDREVGRw",
        "key.json",
        "AK-123456",
        "ACC-123456",
        "PK-123456",
        "S3cret",
        "ZZZ999",
        "YYY888",
        "XXX777"
      )
        .foreach(secret => v should not include secret)
    }
    masked("pgUrl") shouldBe "jdbc:postgresql://u:***@h:5432/db"
    masked("httpUrl") shouldBe "https://u:***@h/x"
  }

  it should "scrub userinfo and cloud credential values from messages" in {
    val options = Map("jsonKeyBase64" -> "QUJDREVGRw==", "apiKey" -> "AK-123456")
    val msg = CatalogScrubber.scrub(
      "failed QUJDREVGRw== AK-123456 at jdbc:postgresql://u:S3cret@h:5432/db and https://u:S3cret@h/x?access_key=ACC1",
      options
    )
    List("QUJDREVGRw", "AK-123456", "S3cret", "ACC1").foreach(secret =>
      msg should not include secret
    )
  }

  "CatalogScrubber.scrub" should "mask values of sensitive options" in {
    val options = Map("url" -> "jdbc:postgresql://h/db", "user" -> "bob", "password" -> "s3cr3t!")
    CatalogScrubber.scrub("auth failed for bob with s3cr3t!", options) shouldBe
    "auth failed for bob with ***"
  }

  it should "mask password parameters embedded in a URL" in {
    CatalogScrubber.scrub(
      "cannot open jdbc:postgresql://h/db?user=bob&password=abc123&ssl=true",
      Map.empty
    ) shouldBe "cannot open jdbc:postgresql://h/db?user=bob&password=***&ssl=true"
  }

  it should "mask token, secret, passwd and api key parameters in a URL" in {
    CatalogScrubber.scrub(
      "failed https://h/x?TOKEN=abc&secret=def;passwd=ghi&api_key=jkl&apikey=mno&ok=1",
      Map.empty
    ) shouldBe "failed https://h/x?TOKEN=***&secret=***;passwd=***&api_key=***&apikey=***&ok=1"
  }

  it should "survive a null message" in {
    CatalogScrubber.scrub(null, Map("password" -> "x")) shouldBe ""
  }
}
