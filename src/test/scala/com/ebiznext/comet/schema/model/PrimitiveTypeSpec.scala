package com.ebiznext.comet.schema.model

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.TestHelper
import PrimitiveType.timestamp

class PrimitiveTypeSpec extends TestHelper {

  new WithSettings() {

    "Timestamp primitive type initialization with null values " should "be empty" in {
      Option(timestamp.fromString(null, null, null)) shouldBe None
      Option(timestamp.fromString("", null, null)) shouldBe None
    }

    "Parsing a long with the epoch_second pattern" should "return a valid timestamp" in {
      val expected: Timestamp = Timestamp.valueOf("2020-5-13 18:41:38")
      val actual = timestamp.fromString("1589388098", timeFormat = "epoch_second")
      actual shouldEqual expected
    }

    "Parsing a long with the epoch_milli pattern" should "return a valid timestamp" in {
      val expected: Timestamp = Timestamp.valueOf("2020-05-13 18:43:23.388")
      timestamp.fromString("1589388203388", timeFormat = "epoch_milli") shouldEqual expected
    }

    // ========================== BASIC_ISO_DATE @UTC =================================
    "Parsing with BASIC_ISO_DATE at UTC zone" should "return a valid timestamp" in {
      val inputDate = "20111203"
      val expected = Timestamp.from(Instant.parse("2011-12-03T00:00:00Z"))
      //      val expected = Timestamp.valueOf("2011-12-03 00:00:00.000")
      val actual = timestamp.fromString(inputDate, "BASIC_ISO_DATE")

      println(s"Expected: ${expected}")
      println(s"Actual: ${actual.asInstanceOf[Timestamp]}")

      println(s"Expected long value: ${expected.getTime}")
      println(s"Actual long value: ${actual.asInstanceOf[Timestamp].getTime}")

      actual shouldEqual expected
    }

    // ========================== BASIC_ISO_DATE @UTC+2 =================================
    "Parsing with BASIC_ISO_DATE at zone UTC+2" should "return a valid timestamp" in {
      val inputDate = "20111203"
      val expected = Timestamp.from(Instant.parse("2011-12-02T22:00:00Z"))
      //      val expected = Timestamp.valueOf("2011-12-02 22:00:00.000")
      val actual = timestamp.fromString(inputDate, "BASIC_ISO_DATE", "UTC+2")

      println(s"Expected: ${expected}")
      println(s"Actual: ${actual.asInstanceOf[Timestamp]}")

      println(s"Expected long value: ${expected.getTime}")
      println(s"Actual long value: ${actual.asInstanceOf[Timestamp].getTime}")

      actual shouldEqual expected
    }

    // ========================== ISO_LOCAL_DATE_TIME @UTC =================================
    "Parsing with ISO_LOCAL_DATE_TIME at UTC" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30"
      val expected = Timestamp.from(Instant.parse("2011-12-03T10:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_LOCAL_DATE_TIME")

      println(s"Expected: ${expected}")
      println(s"Actual: ${actual.asInstanceOf[Timestamp]}")

      println(s"Expected long value: ${expected.getTime}")
      println(s"Actual long value: ${actual.asInstanceOf[Timestamp].getTime}")

      actual shouldEqual expected
    }

    // ========================== ISO_LOCAL_DATE_TIME @UTC+2 =================================
    "Parsing with ISO_LOCAL_DATE_TIME at UTC+2" should "return a valid timestamp" in {
      val input = "2011-12-03T10:15:30"
      val expected = Timestamp.from(Instant.parse("2011-12-03T08:15:30.000Z"))
      val actual = timestamp.fromString(input, "ISO_LOCAL_DATE_TIME", "UTC+2")

      println(s"Expected: ${expected}")
      println(s"Actual: ${actual.asInstanceOf[Timestamp]}")

      println(s"Expected long value: ${expected.getTime}")
      println(s"Actual long value: ${actual.asInstanceOf[Timestamp].getTime}")

      actual shouldEqual expected
    }

//    "Parsing a string with the ISO_LOCAL_DATE standard date pattern" should "return a valid timestamp" in {
//      val value2 = "2011-12-03"
//      timestamp.fromString(value2, "ISO_LOCAL_DATE") shouldEqual
//      Timestamp.valueOf("2011-12-03 00:00:00.0")
//      timestamp.fromString(value2, "ISO_LOCAL_DATE", "UTC+2") shouldEqual
//      Timestamp.valueOf("2011-12-03 00:00:00.0")
//    }
//
//    "Parsing a string with the ISO_OFFSET_DATE standard date pattern" should "return a valid timestamp" in {
//
//      val value3 = "2011-12-03+01:00"
//      timestamp.fromString(value3, "ISO_OFFSET_DATE") shouldEqual
//      Timestamp.valueOf("2011-12-03 00:00:00.0")
//      timestamp.fromString(value3, "ISO_OFFSET_DATE", "UTC+2") shouldEqual
//      Timestamp.valueOf("2011-12-03 00:00:00.0")
//
//    }
//
//    "Parsing a string with the ISO_DATE standard date pattern" should "return a valid timestamp" in {
//
//      val value3 = "2011-12-03+01:00"
//      timestamp.fromString(value3, "ISO_DATE") shouldEqual
//      Timestamp.valueOf("2011-12-03 00:00:00.0")
//      timestamp.fromString(value3, "ISO_DATE", "UTC+2") shouldEqual
//      Timestamp.valueOf("2011-12-03 00:00:00.0")
//
//    }
//
//    "Parsing a string with the ISO_LOCAL_DATE_TIME standard date pattern" should "return a valid timestamp" in {
//
//      val value4 = "2011-12-03T10:15:30"
//      timestamp.fromString(value4, "ISO_LOCAL_DATE_TIME") shouldEqual
//      Timestamp.valueOf("2011-12-03 10:15:30.0")
//      timestamp.fromString(value4, "ISO_LOCAL_DATE_TIME", "UTC+1") shouldEqual
//      Timestamp.valueOf("2011-12-03 10:15:30.0")
//
//    }
//
//    "Parsing a string with the ISO_OFFSET_DATE_TIME standard date pattern" should "return a valid timestamp" in {
//
//      val value5 = "2011-12-03T10:15:30+01:00"
//      timestamp.fromString(value5, "ISO_OFFSET_DATE_TIME") shouldEqual
//      Timestamp.valueOf("2011-12-03 10:15:30.0")
//      timestamp.fromString(value5, "ISO_OFFSET_DATE_TIME", "UTC+2") shouldEqual
//      Timestamp.valueOf("2011-12-03 10:15:30.0")
//
//    }
//
//    "Parsing a string with the ISO_ZONED_DATE_TIME standard date pattern" should "return a valid timestamp" in {
//
//      val value6 = "2011-12-03T10:15:30+01:00[Europe/Paris]"
//      timestamp.fromString(value6, "ISO_ZONED_DATE_TIME") shouldEqual
//      Timestamp.valueOf("2011-12-03 10:15:30.0")
//      timestamp.fromString(
//        value6,
//        "ISO_ZONED_DATE_TIME",
//        "UTC+1"
//      ) shouldEqual
//      Timestamp.valueOf("2011-12-03 10:15:30.0")
//
//    }
//
//    "Parsing a string with the ISO_DATE_TIME standard date pattern" should "return a valid timestamp" in {
//
//      val value6 = "2011-12-03T10:15:30+01:00[Europe/Paris]"
//      timestamp.fromString(value6, "ISO_DATE_TIME") shouldEqual
//      Timestamp.valueOf("2011-12-03 10:15:30.0")
//      timestamp.fromString(value6, "ISO_DATE_TIME", "UTC+1") shouldEqual
//      Timestamp.valueOf("2011-12-03 10:15:30.0")
//
//    }
//
//    "Parsing a string with the ISO_ORDINAL_DATE standard date pattern" should "return a valid timestamp" in {
//
//      val value7 = "2012-337"
//      timestamp.fromString(value7, "ISO_ORDINAL_DATE") shouldEqual
//      Timestamp.valueOf("2012-12-02 00:00:00.0")
//      timestamp.fromString(value7, "ISO_ORDINAL_DATE", "UTC") shouldEqual
//      Timestamp.valueOf("2012-12-02 00:00:00.0")
//
//    }
//
//    "Parsing a string with the ISO_WEEK_DATE standard date pattern" should "return a valid timestamp" in {
//
//      val value8 = "2012-W48-6"
//      timestamp.fromString(value8, "ISO_WEEK_DATE") shouldEqual
//      Timestamp.valueOf("2012-12-01 00:00:00.0")
//      timestamp.fromString(value8, "ISO_WEEK_DATE", "UTC") shouldEqual
//      Timestamp.valueOf("2012-12-01 00:00:00.0")
//
//    }
//
//    "Parsing a string with the ISO_INSTANT standard date pattern" should "return a valid timestamp" in {
//
//      val value9 = "2011-12-03T10:15:30Z"
//      timestamp.fromString(value9, "ISO_INSTANT") shouldEqual
//      Timestamp.valueOf("2011-12-03 11:15:30.0")
//      timestamp.fromString(value9, "ISO_INSTANT", "UTC") shouldEqual
//      Timestamp.valueOf("2011-12-03 11:15:30.0")
//
//    }
//
//    "Parsing a string with the RFC_1123_DATE_TIME standard date pattern" should "return a valid timestamp" in {
//
//      val value10 = "Tue, 3 Jun 2008 11:05:30 GMT"
//      timestamp.fromString(value10, "RFC_1123_DATE_TIME") shouldEqual
//      Timestamp.valueOf("2008-06-03 13:05:30.0")
//      timestamp.fromString(value10, "RFC_1123_DATE_TIME", "UTC") shouldEqual
//      Timestamp.valueOf("2008-06-03 13:05:30.0")
//
//    }
//
//    "Parsing a string with a custom date pattern" should "return a valid timestamp" in {
//      timestamp.fromString("2017-Sep-04", "yyyy-MMM-dd") shouldEqual
//      Timestamp.valueOf("2017-09-04 00:00:00.0")
//
//      // Unparsable with DateTimeFormatter API, but valid with SimpleDateFormat
//      timestamp.fromString("2017-SEP-04", "yyyy-MMM-dd") shouldEqual
//      Timestamp.valueOf("2017-09-04 00:00:00.0")
//
//      timestamp.fromString("2017-09-04 10:01:10.123", "yyyy-MM-dd HH:mm:ss.S") shouldEqual
//      Timestamp.valueOf("2017-09-04 10:01:10.123")
//    }
  }
}
