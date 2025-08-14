package ai.starlake.utils

import java.security.SecureRandom
import java.util.Base64
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import javax.crypto.{Cipher, KeyGenerator}

object AESEncryption {
  val redacted = Base64.getEncoder().encodeToString("**REDACTED**".getBytes("UTF-8"))

  def isEncrypted(text: String): Boolean = {
    // Check if the text starts with the redacted prefix
    text.startsWith(redacted + ".")
  }
  def encrypt(text: String, key: String): String = {
    if (!isEncrypted(text)) {
      // Note: The IV is prepended to the encrypted text for decryption purposes
      // This is a common practice to ensure the IV is available during decryption
      // The IV is not secret, it just needs to be unique for each encryption operation
      // so that the same plaintext encrypted multiple times will yield different ciphertexts.
      // The IV is not included in the Base64 encoding, it is prepended to the
      // encrypted data before encoding.
      // The IV is not secret, it just needs to be unique for each encryption operation
      val rnd = new SecureRandom()
      val initVector = new Array[Byte](16)
      rnd.nextBytes(initVector)
      val iv = new IvParameterSpec(initVector)
      val skeySpec = new SecretKeySpec(Base64.getDecoder.decode(key), "AES")

      val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
      cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv)

      val textBytes = text.getBytes()
      val textToCipher = new Array[Byte](textBytes.length + initVector.length)
      System.arraycopy(initVector, 0, textToCipher, 0, initVector.length)
      System.arraycopy(textBytes, 0, textToCipher, initVector.length, textBytes.length)
      // Encrypt the text
      // Finalize the encryption
      val encrypted = cipher.doFinal(textToCipher)
      val encoded = Base64.getEncoder().encodeToString(encrypted)
      redacted + "." + encoded
    } else {
      text // If the text is already encrypted, return it as is
    }
  }

  def decrypt(text: String, key: String): String = {
    // Note: The original text may contain non-printable characters, so it is safe to
    // convert it directly to a string without worrying about character encoding issues
    // The original text is expected to be UTF-8 encoded, so we can safely convert it back
    // to a string using UTF-8 encoding
    // This assumes the original text was UTF-8 encoded before encryption
    // If the original text was encoded in a different character set, you may need to adjust
    // the encoding used here to match the original encoding
    // Decode the Base64 encoded text
    if (isEncrypted(text)) {
      val components = text.split('.')
      if (components.length != 2 || components(0) != redacted) {
        text // If the text does not contain a redacted prefix, return it as is
      } else {
        val decodedText = Base64.getDecoder.decode(components(1))
        // Extract the IV from the beginning of the decoded text
        if (decodedText.length < 16) {
          throw new IllegalArgumentException("Invalid encrypted text: too short to contain IV")
        }
        val initVector = new Array[Byte](16)
        System.arraycopy(decodedText, 0, initVector, 0, initVector.length)
        // The rest of the decoded text is the actual encrypted data
        val encryptedData = new Array[Byte](decodedText.length - initVector.length)
        System.arraycopy(decodedText, initVector.length, encryptedData, 0, encryptedData.length)
        // Initialize the cipher for decryption
        val iv = new IvParameterSpec(initVector)
        val skeySpec = new SecretKeySpec(Base64.getDecoder.decode(key), "AES")
        val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
        cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv)
        // Decrypt the data
        val original = cipher.doFinal(encryptedData)
        // Convert the decrypted byte array back to a string
        new String(original)
      }
    } else {
      text // If the text is not encrypted, return it as is
    }
  }
  def generateSecretKey(): String = {
    val keyGen = KeyGenerator.getInstance("AES")
    keyGen.init(256) // for example, can be 128, 192, or 256 bits
    val secretKey = keyGen.generateKey()
    Base64.getEncoder.encodeToString(secretKey.getEncoded)
  }

  def main(args: Array[String]): Unit = {
    val text = "Hello, World!"
    val keyGen = KeyGenerator.getInstance("AES")
    keyGen.init(256) // for example

    val secretKey = Base64.getEncoder().encodeToString(keyGen.generateKey().getEncoded())
    val encryptedText = AESEncryption.encrypt(text, secretKey)
    println(s"Encrypted: $encryptedText")
    val decryptedText = AESEncryption.decrypt(encryptedText, secretKey)
    println(s"Decrypted: $decryptedText")
  }

}
