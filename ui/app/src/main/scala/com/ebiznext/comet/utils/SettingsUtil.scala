package com.ebiznext.comet.utils

import com.typesafe.scalalogging.LazyLogging
import configs.Result

object SettingsUtil extends LazyLogging {
  implicit class BetterResult[A](r: Result[A]) {
    def result: A = r.valueOrThrow { e =>
      e.messages.foreach(m => {
        logger.error(s"Error while getting conf : $m")
      })
      new IllegalStateException("Configuration was not loaded properly")
    }
  }
}
