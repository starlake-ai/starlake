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

  it should "send INT64 beyond 2^53 as a string" in {
    conv("9007199254740991", StandardSQLTypeName.INT64) shouldBe
    java.lang.Long.valueOf(9007199254740991L)
    conv("9007199254740993", StandardSQLTypeName.INT64) shouldBe "9007199254740993"
    conv("-9007199254740993", StandardSQLTypeName.INT64) shouldBe "-9007199254740993"
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

class BigQueryPreviewCostSpec extends AnyFlatSpec with Matchers {
  import com.google.cloud.bigquery.BigQueryException

  "viewPreviewJob" should "cap the bytes billed at 1 GiB and use standard SQL" in {
    val job = BigQueryCatalogBackend.viewPreviewJob("SELECT 1")
    BigQueryCatalogBackend.MaxPreviewBytesBilled shouldBe (1L << 30)
    job.getMaximumBytesBilled shouldBe java.lang.Long.valueOf(1L << 30)
    job.useLegacySql() shouldBe false
  }

  "isBytesBilledExceeded" should "recognize the reason and wrapped causes" in {
    val limit = new BigQueryException(400, "Query exceeded limit for bytes billed")
    BigQueryCatalogBackend.isBytesBilledExceeded(limit) shouldBe true
    BigQueryCatalogBackend.isBytesBilledExceeded(new RuntimeException("wrapped", limit)) shouldBe
    true
    BigQueryCatalogBackend.isBytesBilledExceeded(
      new BigQueryException(
        400,
        "x",
        new com.google.cloud.bigquery.BigQueryError("bytesBilledLimitExceeded", null, "x")
      )
    ) shouldBe true
    BigQueryCatalogBackend.isBytesBilledExceeded(new RuntimeException("other")) shouldBe false
    BigQueryCatalogBackend.isBytesBilledExceeded(new BigQueryException(404, "Not found")) shouldBe
    false
  }

  "bytesBilledMessage" should "name the view and suggest a worksheet" in {
    BigQueryCatalogBackend.bytesBilledMessage("ds", "v") shouldBe
    "Preview of view ds.v would scan more than 1 GiB; query it from a worksheet instead"
  }
}
