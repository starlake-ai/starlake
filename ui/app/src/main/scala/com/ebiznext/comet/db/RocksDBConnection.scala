package com.ebiznext.comet.db

import com.ebiznext.comet.utils.SerDeUtils
import org.rocksdb.{ RocksDB, WriteOptions }

/**
 * Created by Mourad on 23/07/2018.
 */
class RocksDBConnection(config: RocksDBConfig) {
  import SerDeUtils._
  lazy val db = RocksDB.open(config.toOptions, config.path)
  lazy val writeOptions = new WriteOptions().setDisableWAL(false).setSync(true)

  def close() = {
    db.close()
  }

  def read[V <: AnyRef](key: String)(implicit m: Manifest[V]): Option[V] = this.synchronized {
    deserialize[V](db.get(serialize[String](key)))
  }

  def write[V <: AnyRef](key: String, value: V)(implicit m: Manifest[V]): Unit = this.synchronized {
    db.put(writeOptions, serialize[String](key), deserialize[V](value))
  }

  def delete(key: String): Unit = this.synchronized {
    db.delete(serialize[String](key))
  }

}

/**
 * Companion object of [[RocksDBConnection]]
 */
object RocksDBConnection {
  RocksDB.loadLibrary()
}
