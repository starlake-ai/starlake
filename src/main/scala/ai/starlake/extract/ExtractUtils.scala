package ai.starlake.extract
import com.typesafe.scalalogging.StrictLogging

import scala.collection.GenTraversable
import scala.collection.parallel.ForkJoinTaskSupport

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

  def createForkSupport(
    maxParOpt: Option[Int] = None,
    minForPar: Int = 2
  ): Option[ForkJoinTaskSupport] = {
    val maxPar = maxParOpt.getOrElse(Runtime.getRuntime().availableProcessors())
    if (maxPar < minForPar) { // don't treat as parallel if famine can occurs.
      logger.info(
        s"Not enough in pool to parallelize (minimum: $minForPar). Falling back to sequential"
      )
      None
    } else {
      val forkJoinPool = new java.util.concurrent.ForkJoinPool(maxPar)
      Some(new ForkJoinTaskSupport(forkJoinPool))
    }
  }

  def makeParallel[T](
    list: GenTraversable[T]
  )(implicit fjp: Option[ForkJoinTaskSupport]): GenTraversable[T] = {
    fjp match {
      case Some(pool) =>
        val parList = list.par
        parList.tasksupport = pool
        parList
      case None =>
        list
    }
  }
}
