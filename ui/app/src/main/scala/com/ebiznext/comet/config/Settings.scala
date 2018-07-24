package com.ebiznext.comet.config

import com.ebiznext.comet.db.RocksDBConfig
import com.typesafe.config.{ Config, ConfigFactory }
import configs.Configs

/**
 * Created by Mourad on 24/07/2018.
 */
object Settings {

  import com.ebiznext.comet.utils.SettingsUtil._

  lazy val config: Config = ConfigFactory.load()

  val interface = config.getString("http.interface")
  val port = config.getInt("http.port")

  val rocksDBConfig: RocksDBConfig = Configs[RocksDBConfig].get(config, "rocksDB").result

}
