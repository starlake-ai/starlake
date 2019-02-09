package com.ebiznext.comet.job

/**
  * Several encryption methods used in privacy management
  */
object Encryption {
  private def algo(alg: String, data: String): String = {
    val m = java.security.MessageDigest.getInstance(alg)
    val b = data.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest())
      .toString(16)
      .reverse
      .padTo(32, "0")
      .reverse
      .mkString
  }

  def md5(s: String): String = {
    algo("MD5", s)
  }

  def sha1(s: String): String = {
    algo("SHA-1", s)
  }

  def sha256(s: String): String = {
    algo("SHA-256", s)
  }

  def sha512(s: String): String = {
    algo("SHA-512", s)
  }
}
