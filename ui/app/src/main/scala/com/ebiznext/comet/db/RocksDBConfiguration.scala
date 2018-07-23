package com.ebiznext.comet.db

import org.rocksdb.Options

/**
 * Created by Mourad on 23/07/2018.
 */
case class RocksDBConfiguration(path: String, createIfMissing: Boolean = true) {

  def toOptions: Options = {
    new Options().setCreateIfMissing(createIfMissing)
  }

  object RocksDBConfiguration {

    def apply(location: String): RocksDBConfiguration = new RocksDBConfiguration(location)
    def apply(location: String, createIfMissing: Boolean): RocksDBConfiguration =
      new RocksDBConfiguration(location, createIfMissing)

  }
}
