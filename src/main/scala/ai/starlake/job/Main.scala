package ai.starlake.job

import ai.starlake.config.Settings
import com.ebiznext.comet.job.Main.legacyMain
import com.typesafe.config.ConfigFactory

object Main {
  implicit val settings: Settings = Settings(ConfigFactory.load())
  def main(args: Array[String]): Unit = legacyMain(args)
}
