package ai.starlake.serve

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RequestParamsSpec extends AnyFlatSpec with Matchers {

  "RequestParams.args" should "split PARAMS on spaces" in {
    val payload = RequestParams.parsePayload("""{"PARAMS":"table-names --x y"}""")
    RequestParams.args(payload).toList shouldBe List("table-names", "--x", "y")
  }

  it should "decode ARGUMENTS as a JSON-encoded string array, keeping spaces" in {
    val payload = RequestParams.parsePayload(
      """{"ARGUMENTS":"[\"transform\",\"--query\",\"select 1 as a\"]"}"""
    )
    RequestParams.args(payload).toList shouldBe List("transform", "--query", "select 1 as a")
  }

  it should "accept ARGUMENTS as a real JSON array" in {
    val payload = RequestParams.parsePayload("""{"ARGUMENTS":["catalog-schemas","--connection","duck"]}""")
    RequestParams.args(payload).toList shouldBe List("catalog-schemas", "--connection", "duck")
  }

  it should "prefer PARAMS when both are present" in {
    val payload = RequestParams.parsePayload(
      """{"PARAMS":"version","ARGUMENTS":"[\"catalog-connections\"]"}"""
    )
    RequestParams.args(payload).toList shouldBe List("version")
  }

  it should "fall back to ARGUMENTS when PARAMS is blank" in {
    val payload = RequestParams.parsePayload("""{"PARAMS":"  ","ARGUMENTS":"[\"version\"]"}""")
    RequestParams.args(payload).toList shouldBe List("version")
  }

  it should "reject a payload without PARAMS or ARGUMENTS" in {
    an[IllegalArgumentException] should be thrownBy RequestParams.args(
      RequestParams.parsePayload("""{"ROOT":"/tmp"}""")
    )
  }

  "RequestParams.string" should "stringify non-string values such as the extension's boolean SL_DUCKDB_MODE" in {
    val payload = RequestParams.parsePayload("""{"SL_DUCKDB_MODE":true,"ENV":"dev","X":null}""")
    RequestParams.string(payload, "SL_DUCKDB_MODE") shouldBe Some("true")
    RequestParams.string(payload, "ENV") shouldBe Some("dev")
    RequestParams.string(payload, "X") shouldBe None
    RequestParams.string(payload, "MISSING") shouldBe None
  }
}
