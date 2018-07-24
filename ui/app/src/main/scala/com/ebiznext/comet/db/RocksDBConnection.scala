package com.ebiznext.comet.db

import com.ebiznext.comet.utils.SerDeUtils
import org.rocksdb.{ RocksDB, WriteOptions }

/**
 * Created by Mourad on 23/07/2018.
 */
class RocksDBConnection(config: RocksDBConfiguration) {

  lazy val db = RocksDB.open(config.toOptions, config.path)
  lazy val writeOptions = new WriteOptions().setDisableWAL(false).setSync(true)

  def close() = {
    db.close()
  }

  def read(key: Any): Option[Any] = this.synchronized {
    Option(SerDeUtils.deserialize(db.get(SerDeUtils.serialize(key))))
  }

  def write(key: Any, value: Any) = this.synchronized {
    db.put(writeOptions, SerDeUtils.serialize(key), SerDeUtils.serialize(value))
  }

  def delete(key: Any) = this.synchronized {
    db.delete(SerDeUtils.serialize(key))
  }

}

/**
 * Companion object of [[RocksDBConnection]]
 */
object RocksDBConnection {
  RocksDB.loadLibrary()
}
