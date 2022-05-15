package ai.starlake.utils

import ai.starlake.schema.model.WriteMode
import ai.starlake.schema.model.WriteMode.{APPEND, IGNORE, OVERWRITE}
import ai.starlake.TestHelper
import ai.starlake.schema.model.WriteMode

class UtilsSpec extends TestHelper {
  new WithSettings {
    "Exceptions" should "be returned as string" in {
      val expected = "java.lang.Exception: test"
      Utils.exceptionAsString(new Exception("test")) should startWith(expected)
    }
    "BigQuery Table Creation / Write Mapping" should "Map to correct BQ Mappings" in {
      Utils.getDBDisposition(APPEND, hasMergeKeyDefined = true) should equal(
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      )
      Utils.getDBDisposition(IGNORE, hasMergeKeyDefined = true) should equal(
        ("CREATE_NEVER", "WRITE_EMPTY")
      )
      Utils.getDBDisposition(OVERWRITE, hasMergeKeyDefined = false) should equal(
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      )
      Utils.getDBDisposition(WriteMode.APPEND, hasMergeKeyDefined = false) should equal(
        ("CREATE_IF_NEEDED", "WRITE_APPEND")
      )
      Utils.getDBDisposition(WriteMode.ERROR_IF_EXISTS, hasMergeKeyDefined = false) should equal(
        ("CREATE_IF_NEEDED", "WRITE_EMPTY")
      )
      Utils.getDBDisposition(WriteMode.IGNORE, hasMergeKeyDefined = false) should equal(
        ("CREATE_NEVER", "WRITE_EMPTY")
      )
    }

    "extracts parts of a String" should "match patterns" in {
      // Given
      val TablePathWithFilter = "(.*)\\.comet_filter\\((.*)\\)".r
      val TablePathWithSelect = "(.*)\\.comet_select\\((.*)\\)".r
      val TablePathWithFilterAndSelect = "(.*)\\.comet_select\\((.*)\\)\\.comet_filter\\((.*)\\)".r
      val standardView = "BQ:project.dataset.table3"
      val customView =
        "BQ:project.dataset.table3.comet_filter(partition = '2020-02-11' AND name = 'test')"
      val filterSelectView =
        "BQ:project.dataset.table3.comet_select('name', 'age', 'graduate_program', 'date_of_birth').comet_filter(partition = '2020-02-11' AND name = 'test')"
      val selectView =
        "BQ:project.dataset.table3.comet_select('name', 'age', 'graduate_program', 'date_of_birth')"
      val filter = "partition = '2020-02-11' AND name = 'test'"
      val select = "'name', 'age', 'graduate_program', 'date_of_birth'"
      // When
      val standardResult = standardView match {
        case TablePathWithFilterAndSelect(tablePath, select, filter) => (tablePath, select, filter)
        case TablePathWithFilter(tablePath, filter)                  => (tablePath, filter)
        case TablePathWithSelect(tablePath, select)                  => (tablePath, select)
        case _                                                       => standardView
      }

      val customResult = customView match {
        case TablePathWithFilterAndSelect(tablePath, select, filter) => (tablePath, select, filter)
        case TablePathWithFilter(tablePath, filter)                  => (tablePath, filter)
        case TablePathWithSelect(tablePath, select)                  => (tablePath, select)
        case _                                                       => customView
      }

      val filterSelectResult = filterSelectView match {
        case TablePathWithFilterAndSelect(tablePath, select, filter) => (tablePath, select, filter)
        case TablePathWithFilter(tablePath, filter)                  => (tablePath, filter)
        case TablePathWithSelect(tablePath, select)                  => (tablePath, select)
        case _                                                       => filterSelectView
      }

      val selectResult = selectView match {
        case TablePathWithFilterAndSelect(tablePath, select, filter) => (tablePath, select, filter)
        case TablePathWithFilter(tablePath, filter)                  => (tablePath, filter)
        case TablePathWithSelect(tablePath, select)                  => (tablePath, select)
        case _                                                       => filterSelectView
      }

      // Then
      standardResult shouldEqual standardView
      customResult shouldEqual (standardView, filter)
      filterSelectResult shouldEqual (standardView, select, filter)
      selectResult shouldEqual (standardView, select)
    }
    "Custom format should" should "match patterns" in {
      import ai.starlake.utils.Formatter._
      assert("${key}_and_${key}".richFormat(Map.empty, Map("key" -> "value")) == "value_and_value")
    }

    "ViewParser" should "substitute refs and return view names" in {
      val input =
        """SELECT *
          |FROM ref( myview), ref(yourview)
          |union
          |select ref(herview )""".stripMargin
      val (sql, views) = ViewExtractor.parse(input)
      sql should be("""SELECT *
                         |FROM myview, yourview
                         |union
                         |select herview""".stripMargin)
      views should contain theSameElementsInOrderAs List("myview", "yourview", "herview")
    }
  }
}
