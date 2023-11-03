package com.ebiznext.comet.job

import ai.starlake.config.Settings
import com.typesafe.scalalogging.StrictLogging

object Main extends StrictLogging {
  def main(args: Array[String]): Unit = {
    logger.warn(
      "com.ebiznext.comet.job.Main is deprecated. Please start using ai.starlake.job.Main"
    )
    Thread.sleep(10 * 1000)
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    new ai.starlake.job.Main().run(args)
  }
}
