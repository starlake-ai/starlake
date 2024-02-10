package ai.starlake.extract

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Domain, Metadata, Mode, Schema}
import ai.starlake.utils.YamlSerializer
import better.files.File

import java.sql.DriverManager
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.{Failure, Success}

class ExtractSpec extends TestHelper {
  "JDBC2Yml of all tables" should "should generated all the table schemas in a YML file" in {
    new WithSettings() {
      val metadata = Metadata(
        mode = Some(Mode.STREAM),
        quote = Some("::"),
        directory = Some("/{{domain}}/{{schema}}")
      )
      val domainTemplate = Domain(name = "CUSTOM_NAME", metadata = Some(metadata))
      testSchemaExtraction(
        JDBCSchema(None, "PUBLIC", pattern = Some("{{schema}}-{{table}}.*"))
          .fillWithDefaultValues(),
        settings.appConfig.connections("test-pg").options,
        Some(domainTemplate)
      ) { case (domain, _, _) =>
        assert(domain.metadata.flatMap(_.quote).getOrElse("") == "::")
        assert(domain.metadata.flatMap(_.mode).getOrElse(Mode.FILE) == Mode.STREAM)
        assert(domain.metadata.flatMap(_.directory).isDefined)
      }
    }
  }

  it should "should generated all the table schemas in a YML file without domain template" in {
    new WithSettings() {
      testSchemaExtraction(
        JDBCSchema(None, "PUBLIC", pattern = Some("{{schema}}-{{table}}.*"))
          .fillWithDefaultValues(),
        settings.appConfig.connections("test-pg").options,
        None
      ) { case (domain, _, _) =>
        assert(domain.metadata.isEmpty)
      }
    }
  }

  private def testSchemaExtraction(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    domainTemplate: Option[Domain]
  )(assertOutput: (Domain, Schema, Schema) => Unit) // Domain, Table definition and View definition
  (implicit settings: Settings) = {
    val jdbcOptions = settings.appConfig.connections("test-pg")
    val conn = DriverManager.getConnection(
      jdbcOptions.options("url"),
      jdbcOptions.options("user"),
      jdbcOptions.options("password")
    )
    val sql: String =
      """
        |drop table if exists test_table1 cascade;
        |create table test_table1(ID INT PRIMARY KEY,NAME VARCHAR(500));
        |create view test_view1 AS SELECT NAME FROM test_table1;
        |insert into test_table1 values (1,'A');""".stripMargin
    val st = conn.createStatement()
    st.execute(sql)
    val rs = st.executeQuery("select * from test_table1")
    rs.next
    val row1InsertionCheck = (1 == rs.getInt("ID")) && ("A" == rs.getString("NAME"))
    assert(row1InsertionCheck, "Data not inserted")
    val outputDir: File = File(s"$starlakeTestRoot/extract-without-template")
    implicit val fjp: Option[ForkJoinTaskSupport] = ExtractUtils.createForkSupport()
    new ExtractJDBCSchema(new SchemaHandler(settings.storageHandler())).extractSchema(
      jdbcSchema,
      connectionOptions,
      outputDir,
      domainTemplate,
      None
    )
    val publicOutputDir = outputDir / "PUBLIC"
    val publicPath = publicOutputDir / "_config.sl.yml"
    val domain =
      YamlSerializer.deserializeDomain(
        publicPath.contentAsString,
        publicPath.pathAsString
      ) match {
        case Success(domain) => domain
        case Failure(e)      => throw e
      }
    assert(domain.name == "PUBLIC")

    val tableFile = publicOutputDir / "TEST_TABLE1.sl.yml"
    val table =
      YamlSerializer
        .deserializeSchemaRefs(tableFile.contentAsString, tableFile.pathAsString)
        .tables
        .head
    table.attributes.map(a => a.name -> a.`type`).toSet should contain theSameElementsAs Set(
      "id"   -> "long",
      "name" -> "string"
    )
    table.primaryKey should contain("id")
    table.pattern.pattern() shouldBe "\\QPUBLIC\\E-\\Qtest_table1\\E.*"
    val viewFile = publicOutputDir / "TEST_VIEW1.sl.yml"
    val view =
      YamlSerializer
        .deserializeSchemaRefs(viewFile.contentAsString, viewFile.pathAsString)
        .tables
        .head
    view.pattern.pattern() shouldBe "\\QPUBLIC\\E-\\Qtest_view1\\E.*"
    assertOutput(domain, table, view)
  }

  "JDBCSchemas" should "deserialize correctly" in {
    new WithSettings() {
      val input =
        """
          |extract:
          |  connectionRef: "test-pg" # Connection name as defined in the connections section of the application.conf file
          |  jdbcSchemas:
          |    - catalog: "business" # Optional catalog name in the target database
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
          |      pattern: "{{schema}}-{{table}}.*"
          |""".stripMargin
      val jdbcMapping = File.newTemporaryFile()
      jdbcMapping.overwrite(input)

      val jdbcSchemas =
        YamlSerializer.deserializeJDBCSchemas(jdbcMapping.contentAsString, jdbcMapping.pathAsString)
      assert(jdbcSchemas.jdbcSchemas.nonEmpty)
      jdbcSchemas shouldBe JDBCSchemas(
        connectionRef = Some("test-pg"),
        jdbcSchemas = List(
          JDBCSchema(
            catalog = Some("business"),
            schema = "public",
            tables = List(
              JDBCTable(
                "user",
                List(TableColumn("id", None), TableColumn("email", None)),
                None,
                None,
                Map.empty,
                None,
                None
              ),
              JDBCTable("product", Nil, None, None, Map.empty, None, None),
              JDBCTable("*", Nil, None, None, Map.empty, None, None)
            ),
            tableTypes = List(
              "TABLE",
              "VIEW",
              "SYSTEM TABLE",
              "GLOBAL TEMPORARY",
              "LOCAL TEMPORARY",
              "ALIAS",
              "SYNONYM"
            ),
            template = Some("/my-templates/domain-template.yml"),
            pattern = Some("{{schema}}-{{table}}.*")
          )
        ),
        default = None
      )
    }
  }

  "JDBCSchemas" should "deserialize with global jdbc schema override" in {
    new WithSettings() {
      val input =
        """
          |extract:
          |  connectionRef: "test-pg" # Connection name as defined in the connections section of the application.conf file
          |  globalJdbcSchema:
          |    catalog: "business" # Optional catalog name in the target database
          |    schema: "public" # Database schema where tables are located
          |    tableTypes: # One or many of the types below
          |      - "TABLE"
          |      - "VIEW"
          |    template: "/my-templates/domain-template.yml" # Metadata to use for the generated YML file.
          |    pattern: "{{schema}}-{{table}}.*"
          |    fullExport: true
          |    sanitizeName: true
          |  jdbcSchemas:
          |    - tables:
          |        - name: "user"
          |          columns: # optional list of columns, if not present all columns should be exported.
          |            - id
          |            - email
          |        - name: product # All columns should be exported
          |        - name: "*" # Ignore any other table spec. Just export all tables
          |""".stripMargin
      val jdbcMapping = File.newTemporaryFile()
      jdbcMapping.overwrite(input)

      val jdbcSchemas =
        YamlSerializer.deserializeJDBCSchemas(jdbcMapping.contentAsString, jdbcMapping.pathAsString)
      assert(jdbcSchemas.jdbcSchemas.nonEmpty)
      jdbcSchemas shouldBe JDBCSchemas(
        connectionRef = Some("test-pg"),
        jdbcSchemas = List(
          JDBCSchema(
            catalog = Some("business"),
            schema = "public",
            tables = List(
              JDBCTable(
                "user",
                List(TableColumn("id", None), TableColumn("email", None)),
                None,
                None,
                Map.empty,
                None,
                None
              ),
              JDBCTable("product", Nil, None, None, Map.empty, None, None),
              JDBCTable("*", Nil, None, None, Map.empty, None, None)
            ),
            tableTypes = List(
              "TABLE",
              "VIEW"
            ),
            template = Some("/my-templates/domain-template.yml"),
            pattern = Some("{{schema}}-{{table}}.*"),
            fullExport = Some(true),
            sanitizeName = Some(true)
          )
        ),
        default = Some(
          JDBCSchema(
            catalog = Some("business"),
            schema = "public",
            tables = Nil,
            tableTypes = List(
              "TABLE",
              "VIEW"
            ),
            template = Some("/my-templates/domain-template.yml"),
            pattern = Some("{{schema}}-{{table}}.*"),
            fullExport = Some(true),
            sanitizeName = Some(true)
          )
        )
      )
    }
  }

  "JDBCSchemas" should "deserialize with default value" in {
    new WithSettings() {
      val input =
        """
          |extract:
          |  connectionRef: "test-pg" # Connection name as defined in the connections section of the application.conf file
          |  globalJdbcSchema:
          |    catalog: "business" # Optional catalog name in the target database
          |    template: "/my-templates/domain-template.yml" # Metadata to use for the generated YML file.
          |    pattern: "{{schema}}-{{table}}.*"
          |  jdbcSchemas:
          |    - tables:
          |        - name: "user"
          |          columns: # optional list of columns, if not present all columns should be exported.
          |            - id
          |            - email
          |        - name: product # All columns should be exported
          |        - name: "*" # Ignore any other table spec. Just export all tables
          |""".stripMargin
      val jdbcMapping = File.newTemporaryFile()
      jdbcMapping.overwrite(input)

      val jdbcSchemas =
        YamlSerializer.deserializeJDBCSchemas(jdbcMapping.contentAsString, jdbcMapping.pathAsString)
      assert(jdbcSchemas.jdbcSchemas.nonEmpty)
      jdbcSchemas shouldBe JDBCSchemas(
        connectionRef = Some("test-pg"),
        jdbcSchemas = List(
          JDBCSchema(
            catalog = Some("business"),
            tables = List(
              JDBCTable(
                "user",
                List(TableColumn("id", None), TableColumn("email", None)),
                None,
                None,
                Map.empty,
                None,
                None
              ),
              JDBCTable("product", Nil, None, None, Map.empty, None, None),
              JDBCTable("*", Nil, None, None, Map.empty, None, None)
            ),
            tableTypes = List(
              "TABLE",
              "VIEW",
              "SYSTEM TABLE",
              "GLOBAL TEMPORARY",
              "LOCAL TEMPORARY",
              "ALIAS",
              "SYNONYM"
            ),
            template = Some("/my-templates/domain-template.yml"),
            pattern = Some("{{schema}}-{{table}}.*"),
            fullExport = Some(false),
            sanitizeName = Some(false)
          )
        ),
        default = Some(
          JDBCSchema(
            catalog = Some("business"),
            tables = Nil,
            tableTypes = Nil,
            template = Some("/my-templates/domain-template.yml"),
            pattern = Some("{{schema}}-{{table}}.*")
          )
        )
      )
    }
  }

  "JDBC2Yml of some columns" should "should generate only the tables and columns requested" in {
    new WithSettings() {
      val jdbcOptions = settings.appConfig.connections("test-pg")
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
      implicit val fjp: Option[ForkJoinTaskSupport] = ExtractUtils.createForkSupport()
      val tmpDir = File.newTemporaryDirectory()
      new ExtractJDBCSchema(new SchemaHandler(settings.storageHandler())).extractSchema(
        JDBCSchema(
          None,
          "PUBLIC",
          None,
          None,
          List(
            JDBCTable(
              "TEST_TABLE1",
              List(TableColumn("id", None)),
              None,
              None,
              Map.empty,
              None,
              None
            )
          )
        ).fillWithDefaultValues(),
        settings.appConfig.connections("test-pg").options,
        tmpDir,
        None,
        None
      )
      val publicPath = tmpDir / "PUBLIC/_config.sl.yml"
      val domain =
        YamlSerializer.deserializeDomain(
          publicPath.contentAsString,
          publicPath.pathAsString
        ) match {
          case Success(domain) => domain
          case Failure(e)      => throw e
        }
      assert(domain.name == "PUBLIC")
      val tableFile = tmpDir / "PUBLIC" / "TEST_TABLE1.sl.yml"
      val table =
        YamlSerializer
          .deserializeSchemaRefs(tableFile.contentAsString, tableFile.pathAsString)
          .tables
          .head
      table.attributes
        .map(_.name) should contain theSameElementsAs Set("id")
      table.attributes.map(_.`type`) should contain theSameElementsAs Set("long")
      table.primaryKey should contain("id")
    }
  }

  "JDBC2Yml with foreign keys" should "detect the foreign keys" in {
    new WithSettings() {
      val jdbcOptions = settings.appConfig.connections("test-pg")
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
      implicit val fjp: Option[ForkJoinTaskSupport] = ExtractUtils.createForkSupport()
      val tmpDir = File.newTemporaryDirectory()
      new ExtractJDBCSchema(new SchemaHandler(settings.storageHandler())).extractSchema(
        JDBCSchema(
          None,
          "PUBLIC",
          None,
          None,
          List(JDBCTable("TEST_TABLE2", Nil, None, None, Map.empty, None, None))
        ).fillWithDefaultValues(),
        settings.appConfig.connections("test-pg").options,
        tmpDir,
        None,
        None
      )
      val publicPath = tmpDir / "PUBLIC" / "_config.sl.yml"
      val domain =
        YamlSerializer.deserializeDomain(
          publicPath.contentAsString,
          publicPath.pathAsString
        ) match {
          case Success(domain) => domain
          case Failure(e)      => throw e
        }
      assert(domain.name == "PUBLIC")
      val tableFile = File(tmpDir / "PUBLIC", "TEST_TABLE2.sl.yml")
      val table =
        YamlSerializer
          .deserializeSchemaRefs(tableFile.contentAsString, tableFile.pathAsString)
          .tables
          .head
      table.attributes
        .find(_.name == "table1_id")
        .get
        .foreignKey
        .getOrElse("") should be("public.test_table1.id")
    }
  }

  "ExtractData Config" should "work" in {
    val rendered = ExtractDataCmd.usage()
    val expected =
      s"""
        |Usage: starlake extract-data [options]
        |
        |
        |Extract data from any database defined in mapping file.
        |
        |Extraction is done in parallel by default and use all the available processors. It can be changed using `parallelism` CLI config.
        |Extraction of a table can be divided in smaller chunk and fetched in parallel by defining partitionColumn and its numPartitions.
        |
        |Examples
        |========
        |
        |Objective: Extract data
        |
        |  starlake.sh extract-data --config my-config --outputDir $$PWD/output
        |
        |Objective: Plan to fetch all data but with different scheduling (once a day for all and twice a day for some) with failure recovery like behavior.
        |  starlake.sh extract-data --config my-config --outputDir $$PWD/output --includeSchemas aSchema
        |         --includeTables table1RefreshedTwiceADay,table2RefreshedTwiceADay --ifExtractedBefore "2023-04-21 12:00:00"
        |         --clean
        |
        |
        |  --config <value>         Database tables & connection info
        |  --limit <value>          Limit number of records
        |  --numPartitions <value>  parallelism level regarding partitionned tables
        |  --parallelism <value>    parallelism level of the extraction process. By default equals to the available cores: ${Runtime
          .getRuntime()
          .availableProcessors()}
        |  --clean                  Clean all files of table only when it is extracted.
        |  --outputDir <value>     Where to output csv files
        |  --incremental            Export only new data since last extraction.
        |  --ifExtractedBefore <value>
        |                           DateTime to compare with the last beginning extraction dateTime. If it is before that date, extraction is done else skipped.
        |  --includeSchemas schema1,schema2
        |                           Domains to include during extraction.
        |  --excludeSchemas schema1,schema2...
        |                           Domains to exclude during extraction. if `include-domains` is defined, this config is ignored.
        |  --includeTables table1,table2,table3...
        |                           Schemas to include during extraction.
        |  --excludeTables table1,table2,table3...
        |                           Schemas to exclude during extraction. if `include-schemas` is defined, this config is ignored.
        |""".stripMargin
    println(expected)
    rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
      .replaceAll("\\s", "")
  }

  "ExtractSchema Config" should "work" in {
    val rendered = ExtractJDBCSchemaCmd.usage()
    println(rendered)
    val expected =
      """
        |Usage: starlake extract-schema [options]
        |  --config <value>        Database tables & connection info
        |  --outputDir <value>    Where to output YML files
        |  --parallelism <value>  parallelism level of the extraction process. By default equals to the available cores
        |""".stripMargin
    rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
      .replaceAll("\\s", "")
  }
}
