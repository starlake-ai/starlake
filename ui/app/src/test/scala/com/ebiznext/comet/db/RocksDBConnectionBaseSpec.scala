package com.ebiznext.comet.db
import better.files._
import com.ebiznext.comet.config.Settings
import org.rocksdb.RocksDB
import org.scalatest._

import scala.collection.mutable

/**
 * Created by Mourad on 31/07/2018.
 */
trait RocksDBConnectionSpecUtils {

  class RocksDBConnectionLike extends RocksDBConnection(Settings.rocksDBConfig)

  class RocksDBConnectionMock extends RocksDBConnectionLike {

    override def close(): Unit = {
      // dummy close
    }
    override def read[V <: AnyRef](key: String)(implicit m: Manifest[V]): Option[V] =
      RocksDBConnectionMock.dbMocked.get(key).asInstanceOf[Option[V]]
    override def write[V <: AnyRef](key: String, value: V)(implicit m: Manifest[V]): Unit =
      RocksDBConnectionMock.dbMocked.put(key, value)
    override def delete(key: String): Unit = RocksDBConnectionMock.dbMocked.remove(key)
  }

  object RocksDBConnectionMock {

    val dbMocked: mutable.HashMap[String, AnyRef] = mutable.HashMap.empty
  }
}

trait RocksDBConnectionBaseSpec
    extends TestSuite
    with Matchers
    with RocksDBConnectionSpecUtils
    with BeforeAndAfterAll
    with BeforeAndAfter {

  implicit lazy val rocksdbConnection: RocksDBConnectionLike = new RocksDBConnectionLike()
  lazy val rocksDb: RocksDB = rocksdbConnection.db

  override def afterAll: Unit = {
    rocksDb.close()
    Settings.rocksDBConfig.path.toFile.delete()
  }
}

trait RocksDBConnectionMockBaseSpec
    extends TestSuite
    with Matchers
    with RocksDBConnectionSpecUtils
    with BeforeAndAfterAll
    with BeforeAndAfter {

  implicit lazy val rocksdbConnection: RocksDBConnectionMock = new RocksDBConnectionMock()
  lazy val rocksDb: mutable.HashMap[String, AnyRef] = RocksDBConnectionMock.dbMocked

}
