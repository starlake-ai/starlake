package com.ebiznext.comet.schema.generator

import better.files.File
import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.schema.model.{Domain, Metadata, Mode}

import java.sql.DriverManager
import scala.util.{Failure, Success}

class DDL2YmlSpec extends TestHelper {
  "DDL2Yml of all tables" should "should generated all the table schemas in a YML file" in {
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

      val metadata = Metadata(mode = Some(Mode.STREAM), quote = Some("::"))
      val domainTemplate = Domain("CUSTOM_NAME", "/{domain}/{schema}", metadata = Some(metadata))
      DDL2Yml.run(JDBCSchema("test-h2", None, "PUBLIC", Nil), File("/tmp"), Some(domainTemplate))
      val domain = YamlSerializer.deserializeDomain(File("/tmp", "PUBLIC.comet.yml")) match {
        case Success(domain) => domain
        case Failure(e)      => throw e
      }
      assert(domain.name == "PUBLIC")
      assert(domain.schemas.size == 2)
      assert(domain.metadata.flatMap(_.quote).getOrElse("") == "::")
      assert(domain.metadata.flatMap(_.mode).getOrElse(Mode.FILE) == Mode.STREAM)
      domain.schemas.map(_.name) should contain theSameElementsAs Set("TEST_TABLE1", "TEST_VIEW1")
      domain.schemas
        .find(_.name == "TEST_TABLE1")
        .get
        .attributes
        .map(_.name) should contain theSameElementsAs Set("ID", "NAME")
      domain.schemas
        .find(_.name == "TEST_TABLE1")
        .get
        .attributes
        .map(_.`type`) should contain theSameElementsAs Set(
        "long",
        "string"
      )
    }
  }

  "DDL2Yml of some columns" should "should generated only the tables and columns requested" in {
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

      DDL2Yml.run(
        JDBCSchema("test-h2", None, "PUBLIC", List(JDBCTable("TEST_TABLE1", List("ID")))),
        File("/tmp"),
        None
      )
      val domain = YamlSerializer.deserializeDomain(File("/tmp", "PUBLIC.comet.yml")) match {
        case Success(domain) => domain
        case Failure(e)      => throw e
      }
      assert(domain.name == "PUBLIC")
      assert(domain.schemas.size == 1)
      assert(domain.schemas.head.name == "TEST_TABLE1")
      domain.schemas.head.attributes
        .map(_.name) should contain theSameElementsAs Set("ID")
      domain.schemas.head.attributes.map(_.`type`) should contain theSameElementsAs Set("long")
    }
  }

  "All SchemaGen Config" should "be known and taken  into account" in {
    val rendered = DDL2YmlConfig.usage()
    val expected =
      """
        |Usage: comet ddl2yml [options]
        |
        |  --jdbc-mapping <value>  Database tables & connection info
        |  --output-dir <value>    Where to output YML files
        |  --yml-template <value>  YML template to use YML metadata
        |
        |""".stripMargin
    rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
      .replaceAll("\\s", "")
  }
}
