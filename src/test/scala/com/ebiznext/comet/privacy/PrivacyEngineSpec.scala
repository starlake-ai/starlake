package com.ebiznext.comet.privacy


import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.privacy.{Email, IPv4, IPv6, Initials, Mask, PrivacyEngine}

class PrivacyEngineSpec extends TestHelper {

  new WithSettings() {
    "Parsing a single arg encryption algo" should "succeed" in {
      val (algo, params)  = PrivacyEngine.parse("com.ebiznext.comet.privacy.Approx(10)")
      algo should equal("com.ebiznext.comet.privacy.Approx")
      params.head shouldBe a [Int]
      params should equal (List(10))
    }
    "Parsing a multiple arg encryption algo" should "succeed" in {
      val (algo, params)  = PrivacyEngine.parse("package.SomeAlgo('X', \"Hello\", 12, false, 12.34)")
      algo should equal("package.SomeAlgo")
      params should have length 5
      params(0) shouldBe a [Char]
      params(1) shouldBe a [String]
      params(2) shouldBe a [Int]
      params(3) shouldBe a [Boolean]
      params(4) shouldBe a [Double]
      params should equal (List('X', "Hello", 12, false, 12.34))
    }

    "Initials Masking Firstname" should "succeed" in {
      val result = Initials.crypt("John")
      result shouldBe "J."
    }

    "Initials Masking Composite name" should "succeed" in {
      val result = Initials.crypt("John Doe")
      result shouldBe "J.D."
    }

    "Email Masking" should "succeed" in {
      val result = Email.crypt("john@doe.com")
      result should have length "527bd5b5d689e2c32ae974c6229ff785@doe.com".length
      result should endWith ("@doe.com")
    }

    "IPv4 Masking" should "succeed" in {
      val result = IPv4.crypt("192.168.2.1")
      result shouldBe "192.168.2.0"
    }

    "IPv4 Masking Multti group" should "succeed" in {
      val result = IPv4.encrypt("192.168.2.1", 2)
      result shouldBe "192.168.0.0"
    }

    "IPv6 Masking" should "succeed" in {
      val result = IPv6.crypt("2001:db8:0:85a3::ac1f:8001")
      result shouldBe "2001:db8:0:85a3::ac1f:0"
    }

    "IPv6 Masking multi group" should "succeed" in {
      val result = IPv6.encrypt("2001:db8:0:85a3::ac1f:8001", 3)
      result shouldBe "2001:db8:0:85a3:0:0:0"
    }

    "Generic Masking" should "succeed" in {
      val result = Mask.crypt("+3360102030405", '*', 8, 4, 2)
      result shouldBe "+336********05"
    }
  }
}
