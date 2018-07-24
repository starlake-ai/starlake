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

  override def afterAll(): Unit = {
    db.close()
    FileUtils.deleteDirectory(tempDir.getAbsoluteFile)
  }

  def getConnection(): RocksDBConnection = {
    val path = tempDir.getAbsolutePath + "/" + UUID.randomUUID()
    new RocksDBConnection(RocksDBConfiguration(path))
  }

  "RocksDb" should "put and get properly" in {
    (1 to 100).foreach { i =>
      db.put(Array(i toByte), s"VALUE$i".toCharArray.map(_.toByte))
    }
    (1 to 100).foreach { i =>
      val actualvalue: String = new String(db.get(Array(i toByte)))
      assert(actualvalue == s"VALUE$i")
    }
  }

  it should "remove properly" in {
    (1 to 100).foreach { i =>
      db.put(Array(i toByte), s"VALUE$i".toCharArray.map(_.toByte))
    }
    (1 to 100).foreach { i =>
      db.remove(Array(i toByte))
      assert(db.get(Array(i toByte)) == null)
    }
  }

  "RocksDbConnection methods" should "work properly" in {
    (1 to 100).foreach { i =>
      conn.write(i, s"VALUE$i")
      val actualvalue: String = conn.read(i).getOrElse("").asInstanceOf[String]
      assert(actualvalue == s"VALUE$i")
      conn.delete(i)
      conn.read(i) match {
        case Some(value) => fail(s"Value $value of key $i should be deleted")
        case None => succeed
      }
    }
  }

}
