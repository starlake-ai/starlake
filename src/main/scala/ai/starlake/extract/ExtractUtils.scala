package ai.starlake.extract
import com.typesafe.scalalogging.StrictLogging

object ExtractUtils extends StrictLogging {

  def timeIt[T](blockLabel: String)(code: => T): T = {
    val startTime = System.currentTimeMillis()
    val result = code
    logger.info(blockLabel + " took " + toHumanElapsedTimeFrom(startTime))
    result
  }

  def toHumanElapsedTimeFrom(startTimeMs: Long): String = {
    val elapsedTimeMs = System.currentTimeMillis() - startTimeMs
    toHumanElapsedTime(elapsedTimeMs)
  }

  def toHumanElapsedTime(elapsedTimeMs: Long): String = {
    if (elapsedTimeMs == 0) {
      "0 ms"
    } else {
      val (output, _) = List(
        "d"  -> 1000 * 60 * 60 * 24,
        "h"  -> 1000 * 60 * 60,
        "m"  -> 1000 * 60,
        "s"  -> 1000,
        "ms" -> 0
      ).foldLeft("" -> elapsedTimeMs) { case ((output, timeMs), (unitSuffix, unitInMs)) =>
        if (elapsedTimeMs < unitInMs)
          output -> timeMs
        else {
          val (elapsedTimeInUnit, restOfTimeMs) = if (unitInMs > 0) {
            val timeAsUnit = timeMs / unitInMs
            val restOfTimeMs = timeMs % unitInMs
            timeAsUnit -> restOfTimeMs
          } else {
            timeMs -> 0L
          }
          if (elapsedTimeInUnit > 0) {
            s"$output $elapsedTimeInUnit$unitSuffix" -> restOfTimeMs
          } else {
            output -> restOfTimeMs
          }
        }
      }
      // remove first space introduced by first concatenation
      output.tail
    }
  }

}
