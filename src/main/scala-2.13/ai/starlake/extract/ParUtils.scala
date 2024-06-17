package ai.starlake.extract

import com.typesafe.scalalogging.StrictLogging

import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ForkJoinTaskSupport

object ParUtils extends StrictLogging {
  def makeParallel[T](
    collection: List[T]
  )(implicit fjp: Option[ForkJoinTaskSupport]) = {
    fjp match {
      case Some(pool) =>
        val parList = collection.par
        parList.tasksupport = pool
        parList
      case None =>
        collection
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
}
