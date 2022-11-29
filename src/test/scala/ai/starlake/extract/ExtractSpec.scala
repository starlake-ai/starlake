package ai.starlake.extract

import ai.starlake.TestHelper
import ai.starlake.schema.model.{Domain, Metadata, Mode}
import ai.starlake.utils.YamlSerializer
import better.files.File

import java.sql.DriverManager
import scala.util.{Failure, Success}

class ExtractSpec extends TestHelper {
  "JDBC2Yml of all tables" should "should generated all the table schemas in a YML file" in {
    new WithSettings() {
      val jdbcOptions = settings.comet.connections("test-h2")
      val conn = DriverManager.getConnection(
        jdbcOptions.options("url"),
        jdbcOptions.options("user"),
        jdbcOptions.options("password")
      )
      val sql: String =
        """
          |drop table if exists test_table1;
          |create table test_table1(ID INT PRIMARY KEY,NAME VARCHAR(500));
          |create view test_view1 AS SELECT NAME FROM test_table1;
          |insert into test_table1 values (1,'A');""".stripMargin
      val st = conn.createStatement()
      st.execute(sql)
      val rs = st.executeQuery("select * from test_table1")
      rs.next
      val row1InsertionCheck = (1 == rs.getInt("ID")) && ("A" == rs.getString("NAME"))
      assert(row1InsertionCheck, "Data not inserted")

      val metadata = Metadata(
        mode = Some(Mode.STREAM),
        quote = Some("::"),
        directory = Some("/{{domain}}/{{schema}}")
      )
      val domainTemplate = Domain(name = "CUSTOM_NAME", metadata = Some(metadata))
      ExtractSchema.extractSchema(
        JDBCSchema(None, "PUBLIC"),
        settings.comet.connections("test-h2").options,
        File("/tmp"),
        Some(domainTemplate)
      )
      val publicPath = File("/tmp/PUBLIC/PUBLIC.comet.yml")
      val domain =
        YamlSerializer.deserializeDomain(
          publicPath.contentAsString,
          publicPath.pathAsString
        ) match {
          case Success(domain) => domain
          case Failure(e)      => throw e
        }
      assert(domain.name == "PUBLIC")
      assert(domain.tableRefs.size == 2)
      assert(domain.metadata.flatMap(_.quote).getOrElse("") == "::")
      assert(domain.metadata.flatMap(_.mode).getOrElse(Mode.FILE) == Mode.STREAM)
      val tableFile = File("/tmp/PUBLIC", "_TEST_TABLE1.comet.yml")
      val table =
        YamlSerializer
          .deserializeSchemas(tableFile.contentAsString, tableFile.pathAsString)
          .tables
          .head
      domain.tableRefs should contain theSameElementsAs Set(
        "_TEST_TABLE1",
        "_TEST_VIEW1"
      )
      table.attributes.map(_.name) should contain theSameElementsAs Set("ID", "NAME")
      table.attributes.map(_.`type`) should contain theSameElementsAs Set("long", "string")
      table.primaryKey should contain("ID")
    }
  }

  "JDBCSchemas" should "deserialize corrected" in {
    new WithSettings() {
      val input =
        """
          |extract:
          |  jdbcSchemas:
          |    - connectionRef: "test-h2" # Connection name as defined in the connections section of the application.conf file
          |      catalog: "business" # Optional catalog name in the target database
          |      schema: "public" # Database schema where tables are located
          |      tables:
          |        - name: "user"
          |          columns: # optional list of columns, if not present all columns should be exported.
          |            - id
          |            - email
          |        - name: product # All columns should be exported
          |        - name: "*" # Ignore any other table spec. Just export all tables
          |      tableTypes: # One or many of the types below
          |        - "TABLE"
          |        - "VIEW"
          |        - "SYSTEM TABLE"
          |        - "GLOBAL TEMPORARY"
          |        - "LOCAL TEMPORARY"
          |        - "ALIAS"
          |        - "SYNONYM"
          |      template: "/my-templates/domain-template.yml" # Metadata to use for the generated YML file.
          |""".stripMargin
      val jdbcMapping = File.newTemporaryFile()
      val outputDir = File.newTemporaryDirectory()
      jdbcMapping.overwrite(input)

      val jdbcSchemas =
        YamlSerializer.deserializeJDBCSchemas(jdbcMapping.contentAsString, jdbcMapping.pathAsString)
      assert(jdbcSchemas.jdbcSchemas.nonEmpty)
    }
  }

  "JDBC2Yml of some columns" should "should generated only the tables and columns requested" in {
    new WithSettings() {
      val jdbcOptions = settings.comet.connections("test-h2")
      val conn = DriverManager.getConnection(
        jdbcOptions.options("url"),
        jdbcOptions.options("user"),
        jdbcOptions.options("password")
      )
      val sql: String =
        """
          |drop view if exists test_view1;
          |drop table if exists test_table1;
          |create table test_table1(ID INT PRIMARY KEY,NAME VARCHAR(500));
          |insert into test_table1 values (1,'A');""".stripMargin
      val st = conn.createStatement()
      st.execute(sql)
      val rs = st.executeQuery("select * from test_table1")
      rs.next
      val row1InsertionCheck = (1 == rs.getInt("ID")) && ("A" == rs.getString("NAME"))
      assert(row1InsertionCheck, "Data not inserted")

      ExtractSchema.extractSchema(
        JDBCSchema(
          None,
          "PUBLIC",
          None,
          None,
          List(JDBCTable("TEST_TABLE1", List("ID")))
        ),
        settings.comet.connections("test-h2").options,
        File("/tmp"),
        None
      )
      val publicPath = File("/tmp/PUBLIC/PUBLIC.comet.yml")
      val domain =
        YamlSerializer.deserializeDomain(
          publicPath.contentAsString,
          publicPath.pathAsString
        ) match {
          case Success(domain) => domain
          case Failure(e)      => throw e
        }
      assert(domain.name == "PUBLIC")
      assert(domain.tableRefs.size == 1)
      assert(domain.tableRefs.head == "_TEST_TABLE1")
      val tableFile = File("/tmp/PUBLIC", "_TEST_TABLE1.comet.yml")
      val table =
        YamlSerializer
          .deserializeSchemas(tableFile.contentAsString, tableFile.pathAsString)
          .tables
          .head
      table.attributes
        .map(_.name) should contain theSameElementsAs Set("ID")
      table.attributes.map(_.`type`) should contain theSameElementsAs Set("long")
      table.primaryKey should contain("ID")
    }
  }

  "JDBC2Yml with foreign keys" should "detect the foreign keys" in {
    new WithSettings() {
      val jdbcOptions = settings.comet.connections("test-h2")
      val conn = DriverManager.getConnection(
        jdbcOptions.options("url"),
        jdbcOptions.options("user"),
        jdbcOptions.options("password")
      )
      val sql: String =
        """
          |drop view if exists test_view1;
          |drop table if exists test_table1;
          |drop table if exists test_table2;
          |create table test_table1(ID INT PRIMARY KEY,NAME VARCHAR(500));
          |create table test_table2(ID INT PRIMARY KEY,TABLE1_ID INT,foreign key (TABLE1_ID) references test_table1(ID));
          |insert into test_table1 values (1,'A');
          |insert into test_table2 values (1,1);
          |insert into test_table2 values (2,1);""".stripMargin
      val st = conn.createStatement()
      st.execute(sql)
      val rs = st.executeQuery("select * from test_table1")
      rs.next
      val row1InsertionCheck = (1 == rs.getInt("ID")) && ("A" == rs.getString("NAME"))
      assert(row1InsertionCheck, "Data not inserted")

      ExtractSchema.extractSchema(
        JDBCSchema(
          None,
          "PUBLIC",
          None,
          None,
          List(JDBCTable("TEST_TABLE2", Nil))
        ),
        settings.comet.connections("test-h2").options,
        File("/tmp"),
        None
      )
      val publicPath = File("/tmp/PUBLIC/PUBLIC.comet.yml")
      val domain =
        YamlSerializer.deserializeDomain(
          publicPath.contentAsString,
          publicPath.pathAsString
        ) match {
          case Success(domain) => domain
          case Failure(e)      => throw e
        }
      assert(domain.name == "PUBLIC")
      assert(domain.tableRefs.size == 1)
      assert(domain.tableRefs.head == "_TEST_TABLE2")
      val tableFile = File("/tmp/PUBLIC", "_TEST_TABLE2.comet.yml")
      val table =
        YamlSerializer
          .deserializeSchemas(tableFile.contentAsString, tableFile.pathAsString)
          .tables
          .head
      table.attributes
        .find(_.name == "TABLE1_ID")
        .get
        .foreignKey
        .getOrElse("") should be("PUBLIC.TEST_TABLE1.ID")
    }
  }

  "ExtractData Config" should "work" in {
    val rendered = ExtractDataConfig.usage()
    println(rendered)
    val expected =
      """
        |Usage: starlake extract-data [options]
        |
        |  --mapping <value>  Database tables & connection info
        |  --limit <value>         Limit number of records
        |  --separator <value>     Column separator
        |  --output-dir <value>    Where to output csv files
        |""".stripMargin
    rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
      .replaceAll("\\s", "")
  }

  "ExtractSchema Config" should "work" in {
    val rendered = ExtractSchemaConfig.usage()
    println(rendered)
    val expected =
      """
        |Usage: starlake extract-schema [options]
        |
        |  --mapping <value>        Database tables & connection info
        |  --output-dir <value>    Where to output YML files
        |""".stripMargin
    rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
      .replaceAll("\\s", "")
  }
}
