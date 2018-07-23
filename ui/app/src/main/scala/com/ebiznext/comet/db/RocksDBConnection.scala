package com.ebiznext.comet.db

import org.rocksdb.RocksDB

/**
  * Created by Mourad on 23/07/2018.
  */
class RocksDBConnection(config: RocksDBConfiguration) {

  lazy val db = RocksDB.open(config.toOptions, config.path)

  RocksDB.loadLibrary()

  def close() = {
    db.close()
  }
}
