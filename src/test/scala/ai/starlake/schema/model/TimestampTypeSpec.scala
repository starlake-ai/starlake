package ai.starlake.schema.model

import java.sql.Timestamp
import java.time.Instant

import ai.starlake.TestHelper
import PrimitiveType.timestamp

class TimestampTypeSpec extends TestHelper {

  new WithSettings {

    "Timestamp primitive type initialization with null values " should "be empty" in {
      Option(timestamp.fromString(null, null, null)) shouldBe None
      Option(timestamp.fromString("", null, null)) shouldBe None
    }

    "Parsing a long with the epoch_second pattern" should "return a valid timestamp" in {
      val expected: Timestamp = Timestamp.from(Instant.parse("2020-05-13T10:15:20Z"))
      val actual = timestamp.fromString("1589364920", timeFormat = "epoch_second")
      actual shouldEqual expected
    }

    "Parsing a long with the epoch_milli pattern" should "return a valid timestamp" in {
      val expected: Timestamp = Timestamp.from(Instant.parse("2020-05-13T10:15:20.388Z"))
      val actual = timestamp.fromString("1589364920388", timeFormat = "epoch_milli")
      actual shouldEqual expected
    }

    "BASIC_ISO_DATE std pattern at UTC timezone" should "return a valid timestamp" in {
      val inputDate = "20111203"
      val expected = Timestamp.from(Instant.parse("2011-12-03T00:00:00Z"))
      val actual = timestamp.fromString(inputDate, "BASIC_ISO_DATE")
      actual shouldEqual expected
    }

    "BASIC_ISO_DATE std pattern at UTC+2 timezone" should "return a valid timestamp" in {
      val inputDate = "20111203"
      val expected = Timestamp.from(Instant.parse("2011-12-02T22:00:00Z"))
      val actual = timestamp.fromString(inputDate, "BASIC_ISO_DATE", "UTC+2")
      actual shouldEqual expected
    }

    "ISO_LOCAL_DATE std pattern at UTC timezone" should "return a valid timestamp" in {
      val input = "2011-12-03"
      val expected = Timestamp.from(Instant.parse("2011-12-03T00:00:00.000Z"))
      val actual = timestamp.fromString(input, "ISO_LOCAL_DATE")
      actual shouldEqual expected
    }

    "ISO_LOCAL_DATE std pattern at UTC+2 timezone" should "return a valid timestamp" in {
      val input = "2011-12-03"
      val expected = Timestamp.from(Instant.parse("2011-12-02T22:00:00.000Z"))
      val actual = timestamp.fromString(input, "ISO_LOCAL_DATE", "UTC+2")
      actual shouldEqual expected
    }

    "ISO_OFFSET_DATE std pattern at UTC timezone" should "return a valid timestamp" in {
      // zone parameter is UTC (default), pattern sets UTC+1
      val input = "2011-12-03+01:00"
      val expected = Timestamp.from(Instant.parse("2011-12-02T23:00:00.000Z"))
      val actual = timestamp.fromString(input, "ISO_OFFSET_DATE")
      actual shouldEqual expected
    }

    "ISO_OFFSET_DATE std pattern at UTC+2 timezone and zone arg set to UTC+2" should "return a valid timestamp" in {
      // zone parameter is UTC+2, pattern sets UTC+2
      val input = "2011-12-03+02:00"
      val expected = Timestamp.from(Instant.parse("2011-12-02T22:00:00.000Z"))
      val actual = timestamp.fromString(input, "ISO_OFFSET_DATE", "UTC+2")
      actual shouldEqual expected
    }

    "ISO_OFFSET_DATE std pattern at UTC+1 timezone and zone arg set to UTC+2" should "fail" in {
      assertThrows[IllegalArgumentException] {
        timestamp.fromString("2011-12-03+01:00", "ISO_OFFSET_DATE", "UTC+2")
      }
    }

    "ISO_DATE std pattern at UTC timezone" should "return a valid timestamp" in {
      val input = "2011-12-03+01:00"
      val expected = Timestamp.from(Instant.parse("2011-12-02T23:00:00.000Z"))
      val actual = timestamp.fromString(input, "ISO_DATE")
      actual shouldEqual expected
    }

    "ISO_DATE std pattern at UTC+2 timezone" should "return a valid timestamp" in {
      val input = "2011-12-03+02:00"
      val expected = Timestamp.from(Instant.parse("2011-12-02T22:00:00.000Z"))
      val actual = timestamp.fromString(input, "ISO_DATE", "UTC+2")
      actual shouldEqual expected
    }

    "ISO_DATE std pattern at UTC+1 timezone with zone arg set to UTC+2" should "fail" in {
      assertThrows[IllegalArgumentException] {
        timestamp.fromString("2011-12-03+01:00", "ISO_DATE", "UTC+2")
      }
    }

    "ISO_LOCAL_DATE_TIME std pattern at UTC timezone" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30"
      val expected = Timestamp.from(Instant.parse("2011-12-03T10:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_LOCAL_DATE_TIME")
      actual shouldEqual expected
    }

    "ISO_LOCAL_DATE_TIME std pattern at UTC+2 timezone" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30"
      val expected = Timestamp.from(Instant.parse("2011-12-03T08:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_LOCAL_DATE_TIME", "UTC+2")
      actual shouldEqual expected
    }
    "ISO_OFFSET_DATE_TIME std pattern at UTC timezone" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30+01:00"
      val expected = Timestamp.from(Instant.parse("2011-12-03T09:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_OFFSET_DATE_TIME")
      actual shouldEqual expected
    }

    "ISO_OFFSET_DATE_TIME std pattern at UTC+2 timezone" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30+02:00"
      val expected = Timestamp.from(Instant.parse("2011-12-03T08:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_OFFSET_DATE_TIME", "UTC+2")
      actual shouldEqual expected
    }

    "ISO_OFFSET_DATE_TIME std pattern at UTC+1 timezone with zone arg set to UTC+2" should "fail" in {
      assertThrows[IllegalArgumentException] {
        timestamp.fromString("2011-12-03T10:15:30+01:00", "ISO_OFFSET_DATE_TIME", "UTC+2")
      }
    }

    "ISO_ZONED_DATE_TIME std pattern at UTC pattern" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30+00:00"
      val expected = Timestamp.from(Instant.parse("2011-12-03T10:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_ZONED_DATE_TIME")
      actual shouldEqual expected
    }

    "ISO_ZONED_DATE_TIME std pattern at UTC+1 timezone" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30+01:00[Europe/Paris]"
      val expected = Timestamp.from(Instant.parse("2011-12-03T09:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_ZONED_DATE_TIME")
      actual shouldEqual expected
    }

    "ISO_ZONED_DATE_TIME pattern at UTC+1 timezone with zone arg set to UTC+1" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30+01:00[Europe/Paris]"
      val expected = Timestamp.from(Instant.parse("2011-12-03T09:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_ZONED_DATE_TIME", "UTC+1")
      actual shouldEqual expected
    }

    "ISO_ZONED_DATE_TIME std pattern at UTC+1 timezone with zone set to UTC+2" should "fail" in {
      assertThrows[IllegalArgumentException] {
        timestamp.fromString(
          "2011-12-03T10:15:30+01:00[Europe/Paris]",
          "ISO_ZONED_DATE_TIME",
          "UTC+2"
        )
      }
    }

    "ISO_DATE_TIME pattern at UTC timezone" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30+00:00"
      val expected = Timestamp.from(Instant.parse("2011-12-03T10:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_DATE_TIME")
      actual shouldEqual expected
    }

    "ISO_DATE_TIME pattern at UTC+1 timezone" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30+01:00[Europe/Paris]"
      val expected = Timestamp.from(Instant.parse("2011-12-03T09:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_DATE_TIME")
      actual shouldEqual expected
    }

    "ISO_DATE_TIME pattern at UTC+1 timezone with zone arg set to UTC+1" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30+01:00[Europe/Paris]"
      val expected = Timestamp.from(Instant.parse("2011-12-03T09:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_DATE_TIME", "UTC+1")
      actual shouldEqual expected
    }

    "ISO_DATE_TIME pattern at UTC+1 timezone with zone arg set to UTC+2" should "fail" in {
      assertThrows[IllegalArgumentException] {
        timestamp.fromString(
          "2011-12-03T10:15:30+01:00[Europe/Paris]",
          "ISO_DATE_TIME",
          "UTC+2"
        )
      }
    }

    "ISO_ORDINAL_DATE standard date pattern at UTC timezone" should "return a valid timestamp" in {
      val input = "2012-337"
      val expected = Timestamp.from(Instant.parse("2012-12-02T00:00:00.000Z"))
      val actual = timestamp.fromString(input, "ISO_ORDINAL_DATE")
      expected shouldEqual actual
    }

    "ISO_ORDINAL_DATE standard date pattern at UTC+2 timezone" should "return a valid timestamp" in {
      val input = "2012-337"
      val expected = Timestamp.from(Instant.parse("2012-12-01T22:00:00.000Z"))
      val actual = timestamp.fromString(input, "ISO_ORDINAL_DATE", "UTC+2")
      expected shouldEqual actual
    }

    "ISO_WEEK_DATE standard date pattern at UTC timezone" should "return a valid timestamp" in {
      val input = "2012-W48-6"
      val expected = Timestamp.from(Instant.parse("2012-12-01T00:00:00.000Z"))
      val actual = timestamp.fromString(input, "ISO_WEEK_DATE")
      expected shouldEqual actual
    }

    "ISO_WEEK_DATE standard date pattern at UTC+2 timezone" should "return a valid timestamp" in {
      val input = "2012-W48-6"
      val expected = Timestamp.from(Instant.parse("2012-11-30T22:00:00.000Z"))
      val actual = timestamp.fromString(input, "ISO_WEEK_DATE", "UTC+2")
      expected shouldEqual actual
    }

    "ISO_INSTANT standard date pattern at UTC timezone" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30Z"
      val expected = Timestamp.from(Instant.parse("2011-12-03T10:15:30Z"))
      val actual = timestamp.fromString(input, "ISO_INSTANT")
      expected shouldEqual actual
    }

    "ISO_INSTANT standard date pattern at UTC+2 timezone" should "fail" in {
      assertThrows[IllegalArgumentException] {
        timestamp.fromString("2011-12-03T10:15:30Z", "ISO_INSTANT", "UTC+2")
      }
    }

    "RFC_1123_DATE_TIME standard date pattern at UTC timezone" should "return a valid timestamp" in {
      val input = "Tue, 3 Jun 2008 11:05:30 GMT"
      val expected = Timestamp.from(Instant.parse("2008-06-03T11:05:30.00Z"))
      val actual = timestamp.fromString(input, "RFC_1123_DATE_TIME")
      expected shouldEqual actual
    }

    "RFC_1123_DATE_TIME standard date pattern at UTC+2 timezone" should "return a valid timestamp" in {
      val input = "Tue, 3 Jun 2008 11:05:30 +0200"
      val expected = Timestamp.from(Instant.parse("2008-06-03T09:05:30.00Z"))
      val actual = timestamp.fromString(input, "RFC_1123_DATE_TIME", "UTC+2")
      expected shouldEqual actual
    }

    "RFC_1123_DATE_TIME standard date pattern at UTC+1 timezone with zone arg set to UTC+2" should "fail" in {
      assertThrows[IllegalArgumentException] {
        timestamp.fromString("Tue, 3 Jun 2008 11:05:30 +0100", "RFC_1123_DATE_TIME", "UTC+2")
      }
    }

    "Custom date pattern at UTC timezone" should "return a valid timestamp" in {
      val input = "2017-Sep-04"
      val expected = Timestamp.from(Instant.parse("2017-09-04T00:00:00.00Z"))
      val actual = timestamp.fromString(input, "yyyy-MMM-dd")
      expected shouldEqual actual
    }

    "Custom date pattern at UTC+2 timezone" should "return a valid timestamp" in {
      val input = "2017-Sep-04"
      val expected = Timestamp.from(Instant.parse("2017-09-03T22:00:00.00Z"))
      val actual = timestamp.fromString(input, "yyyy-MMM-dd", "UTC+2")
      expected shouldEqual actual
    }

    "Legacy date pattern at UTC timezone" should "return a valid timestamp" in {
      // Unparsable with DateTimeFormatter API, but valid with SimpleDateFormat
      val input = "2017-SEP-04"
      val expected = Timestamp.from(Instant.parse("2017-09-04T00:00:00.00Z"))
      val actual = timestamp.fromString(input, "yyyy-MMM-dd")
      expected shouldEqual actual
    }

    "Legacy date pattern at UTC timezone with zone arg set to UTC" should "return a valid timestamp" in {
      // Unparsable with DateTimeFormatter API, but valid with SimpleDateFormat
      val input = "2017-SEP-04"
      val expected = Timestamp.from(Instant.parse("2017-09-04T00:00:00.00Z"))
      val actual = timestamp.fromString(input, "yyyy-MMM-dd", "UTC")
      expected shouldEqual actual
    }

    "Legacy date pattern at UTC timezone with zone arg set to UTC+2" should "fail" in {
      assertThrows[IllegalArgumentException] {
        timestamp.fromString("2017-SEP-04", "yyyy-MMM-dd", "UTC+2")
      }
    }

    "Time only pattern at UTC timezone with zone arg set to UTC" should "return a valid timestamp" in {
      val input = "220157"
      val expected = Timestamp.from(Instant.parse("1970-01-01T22:01:57.00Z"))
      val actual = timestamp.fromString(input, "HHmmss", "UTC")
      expected shouldEqual actual
    }
  }
}
