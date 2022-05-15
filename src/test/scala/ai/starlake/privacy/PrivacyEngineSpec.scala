package ai.starlake.privacy

import ai.starlake.TestHelper

class PrivacyEngineSpec extends TestHelper {

  new WithSettings {
    "Parsing a single arg encryption algo" should "succeed" in {
      val (algo, params) = PrivacyEngine.parse("ai.starlake.privacy.Approx(10)")
      algo should equal("ai.starlake.privacy.Approx")
      params.head.toInt shouldBe a[Int]
      params should equal(List("10"))
    }
    "Parsing a multiple arg encryption algo" should "succeed" in {
      val (algo, params) = PrivacyEngine.parse("package.SomeAlgo('X', \"Hello\", 12, false, 12.34)")
      algo should equal("package.SomeAlgo")
      params should have length 5
      params(0) should have length 1
      params(1) shouldBe a[String]
      params(2).toInt shouldBe a[Int]
      params(3).toBoolean shouldBe a[Boolean]
      params(4).toDouble shouldBe a[Double]
      params should equal(List("X", "Hello", "12", "false", "12.34"))
    }

    "Initials Masking Firstname" should "succeed" in {
      val result = Initials.crypt("John", Map.empty, Nil)
      result shouldBe "J."
    }

    "Initials Masking Composite name" should "succeed" in {
      val result = Initials.crypt("John Doe", Map.empty, Nil)
      result shouldBe "J.D."
    }

    "Email Masking" should "succeed" in {
      val result = Email.crypt("john@doe.com", Map.empty, List("MD5"))
      result should have length "527bd5b5d689e2c32ae974c6229ff785@doe.com".length
      result should endWith("@doe.com")
    }

    "IPv4 Masking" should "succeed" in {
      val result = IPv4.crypt("192.168.2.1", Map.empty, List("1"))
      result shouldBe "192.168.2.0"
    }

    "IPv4 Masking Multti group" should "succeed" in {
      val result = IPv4.crypt("192.168.2.1", 2)
      result shouldBe "192.168.0.0"
    }

    "IPv6 Masking" should "succeed" in {
      val result = IPv6.crypt("2001:db8:0:85a3::ac1f:8001", Map.empty, List("1"))
      result shouldBe "2001:db8:0:85a3::ac1f:0"
    }

    "IPv6 Masking multi group" should "succeed" in {
      val result = IPv6.crypt("2001:db8:0:85a3::ac1f:8001", 3)
      result shouldBe "2001:db8:0:85a3:0:0:0"
    }

    "Generic Masking" should "succeed" in {
      val result = Mask.crypt("+3360102030405", '*', 8, 4, 2)
      result shouldBe "+336********05"
    }

    "ApproxDouble" should "succeed" in {
      val result = ApproxDouble.crypt("2.5", Map.empty, List("20"))
      result.toDouble shouldEqual 2.5 +- 0.5
    }

    "ApproxLong" should "succeed" in {
      val result = ApproxLong.crypt("4", Map.empty, List("25"))
      result.toLong.toDouble shouldEqual 4.0 +- 1

    }

    "RandomDouble" should "succeed" in {
      val resultWithoutBounds = RandomDouble.crypt("", Map.empty, List())
      val resultWithBounds = RandomDouble.crypt("", Map.empty, List("10", "20"))
      resultWithoutBounds.toDouble shouldBe 0.5 +- 1
      resultWithBounds.toDouble shouldBe 15.0 +- 5
    }

    "RandomLong" should "succeed" in {
      val resultWithoutBounds = RandomLong.crypt("", Map.empty, List())
      val resultWithBounds = RandomLong.crypt("", Map.empty, List("10", "20"))
      resultWithoutBounds.toLong shouldBe 0L +- Long.MaxValue
      resultWithBounds.toLong shouldBe 15L +- 5
    }

    "RandomInt" should "succeed" in {
      val resultWithoutBounds = RandomInt.crypt("", Map.empty, List())
      val resultWithBounds = RandomInt.crypt("", Map.empty, List("10", "20"))
      resultWithoutBounds.toInt shouldBe 0 +- Int.MaxValue
      resultWithBounds.toInt shouldBe 15 +- 5
    }

    "Hide" should "succeed" in {
      val result = Hide.crypt("Hello", Map.empty, List("X", "5"))
      result shouldBe "XXXXX"

    }
    "Context based crypting" should "succeed" in {
      object ConditionalHide extends PrivacyEngine {
        override def crypt(
          s: String,
          colMap: => Map[String, Option[String]],
          params: List[String]
        ): String = {
          if (colMap.isDefinedAt("col1")) s else ""
        }
      }
      val colMap =
        Map(
          "col1" -> Some("value1"),
          "col2" -> Some("value2"),
          "col3" -> Some("value3"),
          "col4" -> Some("value4")
        )
      val result1 = ConditionalHide.crypt("value2", colMap, Nil)
      result1 shouldBe "value2"

      val result2 = ConditionalHide.crypt("value5", colMap - "col1", Nil)
      result2 shouldBe ""

    }
    "hash functions" should "not have missing leading zeros" in {
      val randomString = "SomeStringThatDoNotProduceLeadingZeroAfterHash"
      // SHA256
      val testStr1SHA256 = "9QGXpjDcIt"
      val testStr2SHA256 = "w3D6vpfT02"
      val hashSHA256 = PrivacyEngine.algo("SHA-256", testStr1SHA256)
      val hash2SHA256 = PrivacyEngine.algo("SHA-256", testStr2SHA256)
      val hashTestSHA256 = PrivacyEngine.algo("SHA-256", randomString)
      hashSHA256.startsWith("0") shouldBe true
      hash2SHA256.startsWith("0") shouldBe true
      println(hash2SHA256)
      hashSHA256.length shouldBe 64
      hash2SHA256.length shouldBe 64

      hashTestSHA256.startsWith("0") shouldBe false
      hashTestSHA256.length shouldBe 64

      // MD 5
      val testStrMD5 = "dzRycgvXb9"
      val hashMD5 = PrivacyEngine.algo("MD5", testStrMD5)
      val hashTestMD5 = PrivacyEngine.algo("MD5", randomString)
      hashMD5.startsWith("0") shouldBe true
      hashMD5.length shouldBe 32

      hashTestMD5.startsWith("0") shouldBe false
      hashTestMD5.length shouldBe 32
      // SHA-1
      val testStrSHA1 = "bHXgtt2xvO"
      val hashSHA1 = PrivacyEngine.algo("SHA-1", testStrSHA1)
      val hashTestSHA1 = PrivacyEngine.algo("SHA-1", randomString)
      hashSHA1.startsWith("0") shouldBe true
      hashSHA1.length shouldBe 40

      hashTestSHA1.startsWith("0") shouldBe false
      hashTestSHA1.length shouldBe 40

      // SHA-512
      val testStrSHA512 = "q9z2ZT5T5w"
      val hashSHA512 = PrivacyEngine.algo("SHA-512", testStrSHA512)
      val hashTestSHA512 = PrivacyEngine.algo("SHA-512", randomString)
      hashSHA512.startsWith("0") shouldBe true
      hashSHA512.length shouldBe 128

      hashTestSHA512.startsWith("0") shouldBe false
      hashTestSHA512.length shouldBe 128

    }
  }
}
