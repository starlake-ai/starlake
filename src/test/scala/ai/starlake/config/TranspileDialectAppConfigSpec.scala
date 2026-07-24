package ai.starlake.config

import ai.starlake.TestHelper
import ai.starlake.exceptions.SchemaValidationException
import ai.starlake.schema.model.ConnectionType
import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.JSQLTranspiler
import ai.starlake.utils.YamlSerde
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.Path

import scala.jdk.CollectionConverters._

class TranspileDialectAppConfigSpec extends TestHelper {

  private def applicationConfig(dialect: String): String =
    s"""
       |version: 1
       |application:
       |  connections:
       |    duckdb:
       |      type: "jdbc"
       |      _transpileDialect: "$dialect"
       |      options:
       |        url: "jdbc:duckdb:/tmp/duckdb.db"
       |""".stripMargin

  "An application config with a connection _transpileDialect" should "pass schema validation" in {
    val node = YamlSerde.deserializeYamlApplicationNode(
      applicationConfig("GOOGLE_BIG_QUERY"),
      "application.sl.yml"
    )
    node
      .path("application")
      .path("connections")
      .path("duckdb")
      .path("_transpileDialect")
      .asText() shouldBe "GOOGLE_BIG_QUERY"
  }

  it should "reach ConnectionInfo._transpileDialect once the application is loaded" in {
    new WithSettings() {
      val appConfig = Settings.loadApplication(
        applicationConfig("GOOGLE_BIG_QUERY"),
        new Path("application.sl.yml"),
        starlakeTestRoot
      )
      appConfig.connections("duckdb")._transpileDialect shouldBe Some("GOOGLE_BIG_QUERY")
    }
  }

  "every JSQLTranspiler dialect" should "be accepted by the application schema" in {
    JSQLTranspiler.Dialect.values().foreach { dialect =>
      withClue(s"dialect ${dialect.name()}:") {
        noException should be thrownBy
        YamlSerde.deserializeYamlApplicationNode(
          applicationConfig(dialect.name()),
          "application.sl.yml"
        )
      }
    }
  }

  "a null _transpileDialect" should "be accepted like any other unset connection property" in {
    new WithSettings() {
      val config =
        """
          |version: 1
          |application:
          |  connections:
          |    duckdb:
          |      type: "jdbc"
          |      _transpileDialect:
          |      options:
          |        url: "jdbc:duckdb:/tmp/duckdb.db"
          |""".stripMargin
      val appConfig =
        Settings.loadApplication(config, new Path("application.sl.yml"), starlakeTestRoot)
      appConfig.connections("duckdb")._transpileDialect shouldBe None
    }
  }

  "an invalid _transpileDialect value" should "be rejected by schema validation" in {
    val ex = intercept[SchemaValidationException] {
      YamlSerde.deserializeYamlApplicationNode(
        applicationConfig("NOT_A_DIALECT"),
        "application.sl.yml"
      )
    }
    ex.getMessage should include("_transpileDialect")
  }

  "the _transpileDialect enum in the schema resources" should "match the JSQLTranspiler dialects" in {
    val dialects = JSQLTranspiler.Dialect.values().map(_.name()).toList.sorted
    val mapper = new ObjectMapper()
    List("/starlake.json", "/starlake_AppConfigV1.json").foreach { resource =>
      val stream = getClass.getResourceAsStream(resource)
      withClue(s"schema resource $resource:") {
        stream should not be null
      }
      val schema = mapper.readTree(stream)
      val schemaDialects = schema
        .path("definitions")
        .path("ConnectionV1")
        .path("properties")
        .path("_transpileDialect")
        .path("enum")
        .elements()
        .asScala
        .filterNot(_.isNull)
        .map(_.asText())
        .toList
      withClue(s"$resource ConnectionV1._transpileDialect enum:") {
        schemaDialects.sorted shouldBe dialects
        schemaDialects.distinct.size shouldBe schemaDialects.size
      }
    }
  }

  "SQLUtils.transpilerDialect" should "resolve a declared dialect and reject an invalid one with a meaningful error" in {
    def connection(dialect: String): ConnectionInfo =
      ConnectionInfo(
        `type` = ConnectionType.JDBC,
        options = Map("url" -> "jdbc:duckdb:/tmp/duckdb.db"),
        _transpileDialect = Some(dialect)
      )
    SQLUtils.transpilerDialect(connection("SNOWFLAKE")) shouldBe JSQLTranspiler.Dialect.SNOWFLAKE
    val ex = intercept[IllegalArgumentException] {
      SQLUtils.transpilerDialect(connection("NOT_A_DIALECT"))
    }
    ex.getMessage should include("NOT_A_DIALECT")
    ex.getMessage should include("GOOGLE_BIG_QUERY")
  }
}
