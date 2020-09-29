package com.ebiznext.comet.utils

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.schema.model.WriteMode

class UtilsSpec extends TestHelper {
  new WithSettings() {
    "Exceptions" should "be returned as string" in {
      val expected = "java.lang.Exception: test"
      Utils.exceptionAsString(new Exception("test")) should startWith(expected)
    }
    "BigQuery Table Creation / Write Mapping" should "Map to correct BQ Mappings" in {
      Utils.getDBDisposition(WriteMode.APPEND, hasMergeKeyDefined = true) should equal(
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      )
      Utils.getDBDisposition(WriteMode.IGNORE, hasMergeKeyDefined = true) should equal(
        ("CREATE_NEVER", "WRITE_EMPTY")
      )
      Utils.getDBDisposition(WriteMode.OVERWRITE, hasMergeKeyDefined = false) should equal(
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
      val TablePathWithFilter = "(.*)\\.comet_filter(.*)".r
      val standardView = "BQ:project.dataset.table3"
      val customView =
        "BQ:project.dataset.table3.comet_filter(partition = '2020-02-11' AND name = 'test')"
      val filter = "(partition = '2020-02-11' AND name = 'test')"
      // When
      val standardResult = standardView match {
        case TablePathWithFilter(tablePath, filter) => (tablePath, filter)
        case _                                      => standardView
      }

      val customResult = customView match {
        case TablePathWithFilter(tablePath, filter) => (tablePath, filter)
        case _                                      => customView
      }

      // Then
      standardResult shouldEqual standardView
      customResult shouldEqual (standardView, filter)
    }
  }

}
