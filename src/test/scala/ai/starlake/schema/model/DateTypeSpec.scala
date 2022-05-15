package ai.starlake.schema.model

import java.time.format.DateTimeParseException

import ai.starlake.TestHelper
import ai.starlake.schema.model.PrimitiveType.date

class DateTypeSpec extends TestHelper {
  new WithSettings {

    "Parsing a string not respecting the pattern with the number of characters " should "fail" in {
      assertThrows[DateTimeParseException] {
        date.fromString(
          "06/03/20090",
          "dd/MM/yyyy"
        )
      }
    }

    "Parsing a string not respecting the pattern with the separator" should "fail" in {
      assertThrows[DateTimeParseException] {
        date.fromString(
          "06.03.20090",
          "dd/MM/yyyy"
        )
      }
    }

    "Parsing a valid string " should "return a valid date" in {
      val expected = java.sql.Date.valueOf("2009-12-30")
      val result = date.fromString(
        "30.12.2009",
        "dd.MM.yyyy"
      )
      result shouldBe expected
    }

    "Parsing a valid string with format MMMyyyy" should "return a valid date" in {
      val expected = java.sql.Date.valueOf("2018-12-01")
      val result = date.fromString(
        "DEC2018",
        "MMMyyyy",
        "en_EN"
      )
      result shouldBe expected

      assertThrows[DateTimeParseException] {
        date.fromString(
          "DEC20888",
          "MMMyyyy",
          "en_EN"
        )
      }
    }
    "Parsing a valid Year-Month pattern" should "return a date with the first day of the month" in {
      val expected = java.sql.Date.valueOf("2009-12-01")
      date.fromString(
        "2009.12",
        "yyyy.MM"
      ) shouldBe expected

      date.fromString(
        "12-2009",
        "MM-yyyy"
      ) shouldBe expected

      assertThrows[DateTimeParseException] {
        date.fromString(
          "12-20090",
          "MM-yyyy"
        )
      }
    }
  }
}
