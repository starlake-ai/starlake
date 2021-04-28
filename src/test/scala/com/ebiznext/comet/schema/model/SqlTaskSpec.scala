package com.ebiznext.comet.schema.model

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SqlTaskSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with StrictLogging {
  "SQL Task file with PRE, SQL AND POST sections" should "be interpreted correctly" in {
    val sqlContent =
      """
        |/* PRESQL */
        |insert into table value('string', 2, 3)
        |/* SQL */
        |select count(*) from table
        |where x = '${value}'
        |/* POSTSQL */
        |
        |""".stripMargin
    val sqlTask = SqlTask(sqlContent)
    sqlTask shouldBe SqlTask(
      Some(List("insert into table value('string', 2, 3)")),
      "select count(*) from table\nwhere x = '${value}'",
      None
    )
  }
  "SQL Task file with no section" should "be interpreted correctly" in {
    val sqlContent =
      """
        |select count(*) from table
        |where x = '${value}'
        |""".stripMargin
    val sqlTask = SqlTask(sqlContent)
    sqlTask shouldBe SqlTask(None, "select count(*) from table\nwhere x = '${value}'", None)
  }
  "SQL Task file with a single PRESQL Section" should "be interpreted correctly" in {
    val sqlContent =
      """
        |/* PRESQL */
        |insert into table value('string', 2, 3)
        |
    |""".stripMargin
    val sqlTask = SqlTask(sqlContent)
    sqlTask shouldBe SqlTask(Some(List("insert into table value('string', 2, 3)")), "", None)
  }
}
