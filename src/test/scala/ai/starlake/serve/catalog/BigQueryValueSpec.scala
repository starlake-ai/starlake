package ai.starlake.serve.catalog

import com.google.cloud.bigquery.{FieldValue, StandardSQLTypeName}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BigQueryValueSpec extends AnyFlatSpec with Matchers {

  private def prim(s: String) = FieldValue.of(FieldValue.Attribute.PRIMITIVE, s)
  private def conv(s: String, t: StandardSQLTypeName) =
    BigQueryCatalogBackend.jsonValue(prim(s), Some(t))

  "BigQueryCatalogBackend.jsonValue" should "type INT64 as Long" in {
    conv("42", StandardSQLTypeName.INT64) shouldBe java.lang.Long.valueOf(42L)
  }

  it should "type FLOAT64 as Double and keep NaN and Infinity as strings" in {
    conv("1.5", StandardSQLTypeName.FLOAT64) shouldBe java.lang.Double.valueOf(1.5)
    conv("NaN", StandardSQLTypeName.FLOAT64) shouldBe "NaN"
    conv("Infinity", StandardSQLTypeName.FLOAT64) shouldBe "Infinity"
  }

  it should "type BOOL as Boolean" in {
    conv("true", StandardSQLTypeName.BOOL) shouldBe java.lang.Boolean.TRUE
    conv("false", StandardSQLTypeName.BOOL) shouldBe java.lang.Boolean.FALSE
  }

  it should "keep NUMERIC, DATE and STRING as strings" in {
    conv("12.340", StandardSQLTypeName.NUMERIC) shouldBe "12.340"
    conv("2026-10-15", StandardSQLTypeName.DATE) shouldBe "2026-10-15"
    conv("abc", StandardSQLTypeName.STRING) shouldBe "abc"
  }

  it should "render TIMESTAMP as ISO-8601 UTC" in {
    conv("1.6E9", StandardSQLTypeName.TIMESTAMP) shouldBe "2020-09-13T12:26:40Z"
    conv("1600000000.123456", StandardSQLTypeName.TIMESTAMP) shouldBe
    "2020-09-13T12:26:40.123456Z"
  }

  it should "map null to null and fall back to the string without a type" in {
    val nullCell = FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)
    Option(
      BigQueryCatalogBackend.jsonValue(nullCell, Some(StandardSQLTypeName.INT64))
    ) shouldBe None
    BigQueryCatalogBackend.jsonValue(prim("42"), None) shouldBe "42"
  }
}
