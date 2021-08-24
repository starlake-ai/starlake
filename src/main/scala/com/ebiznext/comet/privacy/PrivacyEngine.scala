package com.ebiznext.comet.privacy

import java.security.SecureRandom

/** Several encryption methods used in privacy management
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

  /** @param s:
    *   String => Input string to encrypt
    * @param colMap
    *   : Map[String, Option[String]] => Map of all the attributes and their corresponding values
    * @param params:
    *   List[Any] => Parameters passed to the algorithm as defined in the conf file. Parameter
    *   starting with '"' is converted to a string Parameter containing a '.' is converted to a
    *   double Parameter equals to true of false is converted a boolean Anything else is converted
    *   to an int
    * @return
    *   The encrypted string
    */
  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String
}

object Md5 extends PrivacyEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String =
    PrivacyEngine.algo("MD5", s)
}

object Sha1 extends PrivacyEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String =
    PrivacyEngine.algo("SHA-1", s)
}

object Sha256 extends PrivacyEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String =
    PrivacyEngine.algo("SHA-256", s)
}

object Sha512 extends PrivacyEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String =
    PrivacyEngine.algo("SHA-512", s)
}

object Hide extends PrivacyEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String = {
    if (params.isEmpty)
      ""
    else {
      assert(params.length == 2)
      val c = params(0).asInstanceOf[String]
      val i = params(1).asInstanceOf[Int]
      c * i
    }
  }
}

object No extends PrivacyEngine {
  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String = s
}

object Initials extends PrivacyEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String = {
    s.split("\\s+").map(_.substring(0, 1)).mkString("", ".", ".")
  }
}

object Email extends PrivacyEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String = {
    assert(params.length == 1)
    val split = s.split('@')
    PrivacyEngine.algo(params.head.toString, split(0)) + "@" + split(1)
  }
}

trait IP extends PrivacyEngine {
  def separator: Char

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String = {
    assert(params.length == 1)
    crypt(s, params.head.asInstanceOf[Int])
  }

  def crypt(s: String, maskBytes: Int): String = {
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

trait NumericRandomPrivacy extends PrivacyEngine {
  val rnd: SecureRandom

  final def gen(low: Double, up: Double): Double = low + (up - low) * rnd.nextDouble()

  def genUnbounded(): Double

  final def crypt(params: List[Any]): Double = {
    assert(params.length == 2 || params.isEmpty)
    params match {
      case Nil =>
        genUnbounded()
      case lowerBound :: upperBound :: Nil =>
        val low = lowerBound.asInstanceOf[Int].toDouble
        val up = upperBound.asInstanceOf[Int].toDouble
        gen(low, up)
      case _ => throw new Exception("Should never happen!")
    }
  }
}

object RandomDouble extends NumericRandomPrivacy {
  val rnd = new SecureRandom()

  def genUnbounded(): Double = rnd.nextDouble()

  override def crypt(
    s: String,
    colMap: => Map[String, Option[String]],
    params: List[Any]
  ): String = {
    crypt(params).toString
  }
}

object RandomLong extends NumericRandomPrivacy {
  val rnd = new SecureRandom()

  override def genUnbounded(): Double = rnd.nextLong().toDouble

  override def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String =
    (crypt(params) % Long.MaxValue).toLong.toString
}

object RandomInt extends NumericRandomPrivacy {
  val rnd = new SecureRandom()

  override def genUnbounded(): Double = rnd.nextInt().toDouble

  override def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String =
    (crypt(params) % Int.MaxValue).toInt.toString
}

class ApproxDouble extends PrivacyEngine {
  val rnd = new SecureRandom()

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String = {
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

object ApproxDouble extends ApproxDouble

object ApproxLong extends ApproxDouble {

  override def crypt(
    s: String,
    colMap: => Map[String, Option[String]],
    params: List[Any]
  ): String = {
    assert(params.length == 1)
    crypt(s.toDouble, params.head.asInstanceOf[Int]).toLong.toString
  }

}

object Mask extends PrivacyEngine {

  def crypt(s: String, colMap: => Map[String, Option[String]], params: List[Any]): String = {
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
