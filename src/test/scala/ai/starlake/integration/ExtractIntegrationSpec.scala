package ai.starlake.integration

import ai.starlake.TestHelper
import ai.starlake.extract.{ExtractData, ExtractJDBCSchema}
import ai.starlake.schema.handlers.SchemaHandler
import better.files.File

import java.sql.DriverManager

class ExtractIntegrationSpec extends TestHelper {

  new WithSettings() {
    "Extract Data" should "succeed" in {
      val jdbcOptions = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcOptions.options("url"),
        jdbcOptions.options("user"),
        jdbcOptions.options("password")
      )
      val sqls: String =
        """
          |drop table if exists test_table1 cascade;
          |create table test_table1(ID INT PRIMARY KEY,NAME VARCHAR(500));
          |create view test_view1 AS SELECT NAME FROM test_table1;
          |insert into test_table1 values (1,'A');
          |CREATE TABLE IF NOT EXISTS audit.SL_LAST_EXPORT (
          |                              domain VARCHAR(255) not NULL,
          |                              schema VARCHAR(255) not NULL,
          |                              last_ts TIMESTAMP,
          |                              last_date DATE,
          |                              last_long INTEGER,
          |                              last_decimal DECIMAL,
          |                              start_ts TIMESTAMP not NULL,
          |                              end_ts TIMESTAMP not NULL,
          |                              duration INTEGER not NULL,
          |                              mode VARCHAR(255) not NULL,
          |                              count BIGINT not NULL,
          |                              success BOOLEAN not NULL,
          |                              message VARCHAR(255) not NULL,
          |                              step VARCHAR(255) not NULL
          |                             );""".stripMargin
      val st = conn.createStatement()
      sqls.split(";").foreach { sql =>
        st.executeUpdate(sql)
      }
      st.close()

      val config =
        """
          |extract:
          |  connectionRef: "test-pg" # Connection name as defined in the connections section of the application.conf file
          |  jdbcSchemas:
          |    - schema: "PUBLIC" # Database schema where tables are located
          |      tables:
          |        - name: "*" # Takes all tables
          |      tableTypes: # One or many of the types below
          |        - "TABLE"
          |        - "VIEW"
          |        - "SYSTEM TABLE"
          |        - "GLOBAL TEMPORARY"
          |        - "LOCAL TEMPORARY"
          |        - "ALIAS"
          |        - "SYNONYM"
          |  #  templateFile: "domain-template.yml" # Metadata to use for the generated YML file.
          |""".stripMargin
      val tmpYmlFile = File.newTemporaryFile("extract", ".sl.yml")
      val tmpDir = File.newTemporaryDirectory("extract")
      tmpYmlFile.write(config)
      val schemaHandler = new SchemaHandler(storageHandler)
      new ExtractData(schemaHandler).run(
        Array(
          "--clean",
          "--config",
          tmpYmlFile.pathAsString,
          "--outputDir",
          tmpDir.pathAsString
        )
      )
      val files = tmpDir.listRecursively.filter(_.name.endsWith(".csv")).toList
      assert(files.size == 2)
    }
    "Extract Schema" should "succeed" in {
      val jdbcOptions = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcOptions.options("url"),
        jdbcOptions.options("user"),
        jdbcOptions.options("password")
      )
      val sqls: String =
        """
          |drop view if exists test_view1;
          |drop table if exists test_table1 cascade;
          |create table test_table1(ID INT PRIMARY KEY,NAME VARCHAR(500));
          |create view test_view1 AS SELECT NAME FROM test_table1;
          |insert into test_table1 values (1,'A');
          |CREATE TABLE IF NOT EXISTS audit.SL_LAST_EXPORT (
          |                              domain VARCHAR(255) not NULL,
          |                              schema VARCHAR(255) not NULL,
          |                              last_ts TIMESTAMP,
          |                              last_date DATE,
          |                              last_long INTEGER,
          |                              last_decimal DECIMAL,
          |                              start_ts TIMESTAMP not NULL,
          |                              end_ts TIMESTAMP not NULL,
          |                              duration INTEGER not NULL,
          |                              mode VARCHAR(255) not NULL,
          |                              count BIGINT not NULL,
          |                              success BOOLEAN not NULL,
          |                              message VARCHAR(255) not NULL,
          |                              step VARCHAR(255) not NULL
          |                             );""".stripMargin
      val st = conn.createStatement()
      sqls.split(";").foreach { sql =>
        st.executeUpdate(sql)
      }
      st.close()

      val config =
        """
          |extract:
          |  connectionRef: "test-pg" # Connection name as defined in the connections section of the application.conf file
          |  jdbcSchemas:
          |    - schema: "PUBLIC" # Database schema where tables are located
          |      tables:
          |        - name: "*" # Takes all tables
          |      tableTypes: # One or many of the types below
          |        - "TABLE"
          |        - "VIEW"
          |        - "SYSTEM TABLE"
          |        - "GLOBAL TEMPORARY"
          |        - "LOCAL TEMPORARY"
          |        - "ALIAS"
          |        - "SYNONYM"
          |""".stripMargin
      val tmpYmlFile = File.newTemporaryFile("extract", ".sl.yml")
      val tmpDir = File.newTemporaryDirectory("extract")
      println(tmpDir.pathAsString)
      tmpYmlFile.write(config)
      val schemaHandler = new SchemaHandler(storageHandler)
      new ExtractJDBCSchema(schemaHandler).run(
        Array(
          "--config",
          tmpYmlFile.pathAsString,
          "--outputDir",
          tmpDir.pathAsString
        )
      )
      val files =
        tmpDir.listRecursively.filter(_.name.endsWith(".sl.yml")).map(_.name.toLowerCase()).toList
      files should contain theSameElementsAs List(
        "test_table1.sl.yml",
        "test_view1.sl.yml",
        "_config.sl.yml"
      )
    }

  }
}
