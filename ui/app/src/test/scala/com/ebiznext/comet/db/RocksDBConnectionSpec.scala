package com.ebiznext.comet.db

import java.nio.file.Files
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll, FlatSpec }

/**
 * Created by Mourad on 23/07/2018.
 */
class RocksDBConnectionSpec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll {

  lazy val tempDir = Files.createTempDirectory("comet-test").toFile
  lazy val conn: RocksDBConnection = getConnection()
  val db = conn.db
  val keys = (1 to 100).map(String.valueOf(_))

  override def afterAll(): Unit = {
    db.close()
    FileUtils.deleteDirectory(tempDir.getAbsoluteFile)
  }

  def getConnection(): RocksDBConnection = {
    val path = tempDir.getAbsolutePath + "/" + UUID.randomUUID()
    new RocksDBConnection(RocksDBConfig(path))
  }

  "RocksDb" should "put and get properly" in {
    keys.foreach { i =>
      db.put(Array(i toByte), s"VALUE$i".toCharArray.map(_.toByte))
    }
    keys.foreach { i =>
      val actualvalue: String = new String(db.get(Array(i toByte)))
      assert(actualvalue == s"VALUE$i")
    }
  }

  it should "remove properly" in {
    keys.foreach { i =>
      db.put(Array(i toByte), s"VALUE$i".toCharArray.map(_.toByte))
    }
    keys.foreach { i =>
      db.remove(Array(i toByte))
      assert(db.get(Array(i toByte)) == null)
    }
  }

  "RocksDbConnection methods" should "work properly" in {

    keys.foreach { i =>
      val expected = s"VALUE$i"

      conn.write[String](i, expected)

      val actualValue: String = conn.read[String](i).get
      assert(actualValue == expected)

      conn.delete(i)

      conn.read[String](i) match {
        case Some(value) => fail(s"Value $value of key $i should be deleted")
        case None => succeed
      }
    }
  }

}
