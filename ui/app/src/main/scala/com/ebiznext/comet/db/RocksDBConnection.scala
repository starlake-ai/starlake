package com.ebiznext.comet.db

import com.ebiznext.comet.utils.SerDeUtils
import org.rocksdb.{ RocksDB, WriteOptions }

/**
 * Created by Mourad on 23/07/2018.
 */
class RocksDBConnection(config: RocksDBConfiguration) {
  import SerDeUtils._
  lazy val db = RocksDB.open(config.toOptions, config.path)
  lazy val writeOptions = new WriteOptions().setDisableWAL(false).setSync(true)

  def close() = {
    db.close()
  }

  def read[K, V >: Null <: AnyRef](key: K): V = this.synchronized {
    deserialize(db.get(serialize(key)))
  }

  def write[K, V](key: K, value: V) = this.synchronized {
    db.put(writeOptions, key, value)
  }

  def delete[K](key: K) = this.synchronized {
    db.delete(key)
  }

}

/**
 * Companion object of [[RocksDBConnection]]
 */
object RocksDBConnection {
  RocksDB.loadLibrary()
}
