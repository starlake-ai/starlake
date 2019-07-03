package com.ebiznext.comet.utils

/**
  * Several encryption methods used in privacy management
  */
object Encryption {
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
}

trait Encryption {
  def encrypt(s: String): String
}

object Md5 extends Encryption {
  def encrypt(s: String): String = Encryption.algo("MD5", s)
}

object Sha1 extends Encryption {
  def encrypt(s: String): String = Encryption.algo("SHA-1", s)
}

object Sha256 extends Encryption {
  def encrypt(s: String): String = Encryption.algo("SHA-256", s)
}

object Sha512 extends Encryption {
  def encrypt(s: String): String = Encryption.algo("SHA-512", s)
}

object Hide extends Encryption {
  def encrypt(s: String): String = ""
}

object No extends Encryption {
  def encrypt(s: String): String = s
}
