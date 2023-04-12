package ai.starlake.utils

import com.typesafe.scalalogging.StrictLogging

object DeprecatedChecks extends StrictLogging {
  def cometEnvVars(): Unit = {
    val version = "0.7"
    val cometVars = sys.env.keys.filter(_.startsWith("COMET_"))
    cometVars.foreach { cometVar =>
      val slkVar = "SL_" + cometVar.substring("COMET_".length)
      logger.warn(s"$cometVar is deprecated please use $slkVar (since $version)")
    }
  }
}
