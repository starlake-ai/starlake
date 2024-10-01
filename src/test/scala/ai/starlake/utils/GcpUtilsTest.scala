package ai.starlake.utils
import ai.starlake.job.ingest.RejectedRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class GcpUtilsTest extends AnyFlatSpec with Matchers {

  "GcpUtils" should "adapt input values not supported by Gcp Struct" in {
    val timestamp = java.sql.Timestamp.from(Instant.now())
    val rejectedRecord = RejectedRecord(
      jobid = "job_id",
      timestamp = timestamp,
      domain = "domain",
      schema = "schema",
      error = "error",
      path = "path"
    )
    val rejectedRecordOutput: Map[String, Any] =
      GcpUtils.adapt_map_to_gcp_log(rejectedRecord.asMap())
    rejectedRecordOutput should contain allOf (
      "jobid"     -> "job_id",
      "timestamp" -> timestamp.getTime,
      "domain"    -> "domain",
      "schema"    -> "schema",
      "error"     -> "error",
      "path"      -> "path"
    )

    object DummyEnum extends Enumeration {
      type DummyEnum = Value
      val Is, A, Dummy, Enumeration = Value
    }

    val current_instant = java.time.Instant.now()

    val variousTypes: Map[String, Any] = Map(
      "aString"    -> "aString",
      "aTimestamp" -> timestamp,
      "anInt"      -> 1,
      "aLong"      -> 1L,
      "aFloat"     -> 1.0f,
      "aDouble"    -> 1.0d,
      "anEnum"     -> DummyEnum.Dummy,
      "anInstant"  -> current_instant,
      "aNull"      -> null,
      (null, "Strange null key"),
      "aMap" -> Map(
        "aString"    -> "aString",
        "aTimestamp" -> timestamp,
        "anInt"      -> 1,
        "aLong"      -> 1L,
        "aFloat"     -> 1.0f,
        "aDouble"    -> 1.0d,
        "anEnum"     -> DummyEnum.Dummy,
        "anInstant"  -> current_instant
      ),
      "aList" -> List(
        Map(
          "aString"    -> "aString",
          "aTimestamp" -> timestamp,
          "anInt"      -> 1,
          "aLong"      -> 1L,
          "aFloat"     -> 1.0f,
          "aDouble"    -> 1.0d,
          "anEnum"     -> DummyEnum.Dummy,
          "anInstant"  -> current_instant
        )
      )
    )
    val output: Map[String, Any] = GcpUtils.adapt_map_to_gcp_log(variousTypes)
    output should contain allOf (
      "aString"    -> "aString",
      "aTimestamp" -> timestamp.getTime,
      "anInt"      -> 1,
      "aLong"      -> 1L,
      "aFloat"     -> 1.0f,
      "aDouble"    -> 1.0d,
      "anEnum"     -> DummyEnum.Dummy.toString,
      "anInstant"  -> current_instant.toString,
      "aNull"      -> null,
      (null, "Strange null key"),
      "aMap" -> Map(
        "aString"    -> "aString",
        "aTimestamp" -> timestamp.getTime,
        "anInt"      -> 1,
        "aLong"      -> 1L,
        "aFloat"     -> 1.0f,
        "aDouble"    -> 1.0d,
        "anEnum"     -> DummyEnum.Dummy.toString,
        "anInstant"  -> current_instant.toString
      ),
      "aList" -> List(
        Map(
          "aString"    -> "aString",
          "aTimestamp" -> timestamp.getTime,
          "anInt"      -> 1,
          "aLong"      -> 1L,
          "aFloat"     -> 1.0f,
          "aDouble"    -> 1.0d,
          "anEnum"     -> DummyEnum.Dummy.toString,
          "anInstant"  -> current_instant.toString
        )
      )
    )
  }
}
