package ai.starlake.utils

import ai.starlake.TestHelper
import ai.starlake.schema.model.WriteMode
import ai.starlake.schema.model.WriteMode.{APPEND, OVERWRITE}

class UtilsSpec extends TestHelper {
  new WithSettings() {
    "Exceptions" should "be returned as string" in {
      val expected = "java.lang.Exception: test"
      Utils.exceptionAsString(new Exception("test")) should startWith(expected)
    }
    "BigQuery Table Creation / Write Mapping" should "Map to correct BQ Mappings" in {
      Utils.getDBDisposition(APPEND, hasMergeKeyDefined = true, isJDBC = false) should equal(
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      )
      Utils.getDBDisposition(OVERWRITE, hasMergeKeyDefined = false, isJDBC = false) should equal(
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      )
      Utils.getDBDisposition(
        WriteMode.APPEND,
        hasMergeKeyDefined = false,
        isJDBC = false
      ) should equal(
        ("CREATE_IF_NEEDED", "WRITE_APPEND")
      )
      Utils.getDBDisposition(
        WriteMode.OVERWRITE,
        hasMergeKeyDefined = false,
        isJDBC = false
      ) should equal(
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      )
    }

    "Custom format should" should "match patterns" in {
      import ai.starlake.utils.Formatter._
      assert("${key}_and_${key}".richFormat(Map.empty, Map("key" -> "value")) == "value_and_value")
    }

    "Parse Jinja using env vars" should "match result" in {
      val result =
        Utils.parseJinja(
          "{%if ok %}{{ 'hello' | upper }}{% endif %}",
          Map("ok" -> true)
        )
      assert(result.trim == "HELLO")
    }
  }
}
