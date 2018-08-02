package com.ebiznext.comet.db
import org.scalatest.FlatSpec

/**
 * Created by Mourad on 23/07/2018.
 */
class RocksDBConnectionSpec extends FlatSpec with RocksDBConnectionBaseSpec {

  val keys: List[String] = (1 to 100).map(String.valueOf).toList

  "RocksDb" should "put and get properly" in {
    keys.foreach { i =>
      rocksDb.put(Array(i toByte), s"VALUE$i".toCharArray.map(_.toByte))
    }
    keys.foreach { i =>
      val actualvalue: String = new String(rocksDb.get(Array(i toByte)))
      assert(actualvalue == s"VALUE$i")
    }
  }

  it should "remove properly" in {
    keys.foreach { i =>
      rocksDb.put(Array(i toByte), s"VALUE$i".toCharArray.map(_.toByte))
    }
    keys.foreach { i =>
      rocksDb.delete(Array(i toByte))
      assert(rocksDb.get(Array(i toByte)) == null)
    }
  }

  "RocksDbConnection methods" should "work properly" in {

    keys.foreach { i =>
      val expected = s"VALUE$i"

      rocksdbConnection.write[String](i, expected)

      val actualValue: String = rocksdbConnection.read[String](i).get
      assert(actualValue == expected)

      rocksdbConnection.delete(i)

      rocksdbConnection.read[String](i) match {
        case Some(v) => fail(s"Value $v of key $i should be deleted")
        case None => succeed
      }
    }
  }

}
