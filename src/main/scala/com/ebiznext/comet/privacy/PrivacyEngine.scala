package com.ebiznext.comet.privacy

import scala.util.Random

/**
  * Several encryption methods used in privacy management
  */
object PrivacyEngine {

  def algo(alg: String, data: String): String = {
    val m = java.security.MessageDigest.getInstance(alg)
    val b = data.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest())
      .toString(16)
      .reverse
      .padTo(32, '0')
      .reverse
      .mkString
  }

  def parse(maskingAlgo: String): (String, List[Any]) = {
    def parseParams(params: List[String]): List[Any] =
      params.map { param =>
        if (param.startsWith("\"") && param.endsWith("\""))
          param.substring(1, param.length - 1)
        else if (param.startsWith("'") && param.endsWith("'"))
          param.charAt(1)
        else if (param.contains('.'))
          param.toDouble
        else if (param.equalsIgnoreCase("true") || param.equalsIgnoreCase("false"))
          param.toBoolean
        else
          param.toInt
      }
    val hasParam = maskingAlgo.indexOf('(')
    if (hasParam > 0) {
      assert(maskingAlgo.indexOf(')') > hasParam)
      (
        maskingAlgo.substring(0, hasParam),
        parseParams(
          maskingAlgo.substring(hasParam + 1, maskingAlgo.length - 1).split(',').map(_.trim).toList
        )
      )
    } else {
      (maskingAlgo, Nil)
    }
  }
}

trait PrivacyEngine {
  def crypt(s: String): String
  def crypt(s: String, params: List[Any]): String = crypt(s)
}

object Md5 extends PrivacyEngine {
  def crypt(s: String): String = PrivacyEngine.algo("MD5", s)
}

object Sha1 extends PrivacyEngine {
  def crypt(s: String): String = PrivacyEngine.algo("SHA-1", s)
}

object Sha256 extends PrivacyEngine {
  def crypt(s: String): String = PrivacyEngine.algo("SHA-256", s)
}

object Sha512 extends PrivacyEngine {
  def crypt(s: String): String = PrivacyEngine.algo("SHA-512", s)
}

object Hide extends PrivacyEngine {
  def crypt(s: String): String = ""
}

object No extends PrivacyEngine {
  def crypt(s: String): String = s
}

object Initials extends PrivacyEngine {

  def crypt(s: String): String = {
    s.split("\\s+").map(_.substring(0, 1)).mkString("", ".", ".")
  }
}

object Email extends PrivacyEngine {
  def crypt(s: String): String = crypt(s, List("MD5"))

  override def crypt(s: String, params: List[Any]): String = {
    assert(params.length == 1)
    val split = s.split('@')
    PrivacyEngine.algo(params.head.toString, split(0)) + "@" + split(1)
  }
}

trait IP extends PrivacyEngine {
  def separator: Char
  def crypt(s: String): String = encrypt(s, 1)

  override def crypt(s: String, params: List[Any]): String = {
    assert(params.length == 1)
    encrypt(s, params.head.asInstanceOf[Int])
  }

  def encrypt(s: String, maskBytes: Int): String = {
    val ip = s.split(separator)
    (ip.dropRight(maskBytes) ++ List.fill(maskBytes)(0)).mkString(separator.toString)
  }
}

object IPv4 extends IP {
  override val separator: Char = '.'
}

object IPv6 extends IP {
  override val separator: Char = ':'
}

object Approx extends PrivacyEngine {
  val rnd = new Random()
  override def crypt(s: String): String = crypt(s.toDouble, 100).toString

  override def crypt(s: String, params: List[Any]): String = {
    assert(params.length == 1)
    crypt(s.toDouble, params.head.asInstanceOf[Int]).toString
  }

  def crypt(value: Double, percent: Int): Double = {
    val rndBool = rnd.nextBoolean()
    val distance = (value * percent * rnd.nextDouble()) / 100
    if (rndBool)
      value - distance
    else
      value + distance
  }
}

object Mask extends PrivacyEngine {
  override def crypt(s: String): String = crypt(s, 'X', 8, 1, 1)

  override def crypt(s: String, params: List[Any]): String = {
    assert(params.length == 4)
    val maskingChar = params(0).asInstanceOf[Char]
    val numberOfChars = params(1).asInstanceOf[Int]
    val leftSide = params(2).asInstanceOf[Int]
    val rightSide = params(3).asInstanceOf[Int]
    crypt(s, maskingChar, numberOfChars, leftSide, rightSide)
  }

  def crypt(
    s: String,
    maskingChar: Char,
    numberOfChars: Int,
    leftSide: Int,
    rightSide: Int
  ): String = {
    s match {
      case input if input.length <= leftSide =>
        "%s%s".format(input, maskingChar.toString * numberOfChars)
      case _ =>
        "%s%s%s".format(
          s.take(leftSide),
          maskingChar.toString * numberOfChars,
          s.takeRight(rightSide)
        )
    }
  }
}
