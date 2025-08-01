package ai.starlake.schema.model

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SqlTaskSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with LazyLogging {
  "SQL Task file with PRE, SQL AND POST sections" should "be interpreted correctly" in {
    val sqlContent =
      """
        |-- PRESQL
        |insert into table value('string', 2, 3)
        |-- SQL
        |select count(*) from table
        |where x = '${value}'
        |-- POSTSQL
        |
        |""".stripMargin
    val sqlTask = SqlTaskExtractor(sqlContent)
    sqlTask shouldBe SqlTaskExtractor(
      List("insert into table value('string', 2, 3)"),
      "select count(*) from table\nwhere x = '${value}'",
      Nil
    )
  }
  "SQL Task file with no section" should "be interpreted correctly" in {
    val sqlContent =
      """
        |select count(*) from table
        |where x = '${value}'
        |""".stripMargin
    val sqlTask = SqlTaskExtractor(sqlContent)
    sqlTask shouldBe SqlTaskExtractor(
      Nil,
      "select count(*) from table\nwhere x = '${value}'",
      Nil
    )
  }
  "SQL Task file with a single PRESQL Section" should "be interpreted correctly" in {
    val sqlContent =
      """
        |-- PRESQL
        |insert into table value('string', 2, 3)
        |
    |""".stripMargin
    val sqlTask = SqlTaskExtractor(sqlContent)
    sqlTask shouldBe SqlTaskExtractor(
      List("insert into table value('string', 2, 3)"),
      "",
      Nil
    )
  }
}
