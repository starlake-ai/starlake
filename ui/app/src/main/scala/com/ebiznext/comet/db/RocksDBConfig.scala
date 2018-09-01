package com.ebiznext.comet.db

import org.rocksdb.Options

/**
  * Created by Mourad on 23/07/2018.
  */
final case class RocksDBConfig(path: String, createIfMissing: Boolean = true) {

  def toOptions: Options = {
    new Options().setCreateIfMissing(createIfMissing)
  }

  object RocksDBConfig {

    def apply(location: String): RocksDBConfig = new RocksDBConfig(location)
    def apply(location: String, createIfMissing: Boolean): RocksDBConfig =
      new RocksDBConfig(location, createIfMissing)

  }
}
