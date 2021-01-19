package com.ebiznext.comet.schema.model

import java.time.format.DateTimeParseException

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.schema.model.PrimitiveType.date

class DateTypeSpec extends TestHelper{
  new WithSettings(){

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
  }
}
