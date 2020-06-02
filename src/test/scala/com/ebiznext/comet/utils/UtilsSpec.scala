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
      Utils.getDBDisposition(WriteMode.OVERWRITE) should equal(
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      )
      Utils.getDBDisposition(WriteMode.APPEND) should equal(
        ("CREATE_IF_NEEDED", "WRITE_APPEND")
      )
      Utils.getDBDisposition(WriteMode.ERROR_IF_EXISTS) should equal(
        ("CREATE_IF_NEEDED", "WRITE_EMPTY")
      )
      Utils.getDBDisposition(WriteMode.IGNORE) should equal(
        ("CREATE_NEVER", "WRITE_EMPTY")
      )
    }
  }

}
