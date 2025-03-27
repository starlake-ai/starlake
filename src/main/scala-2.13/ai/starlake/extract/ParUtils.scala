package ai.starlake.extract

import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.ExecutionContext.fromExecutor
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object ParUtils extends StrictLogging {
  def makeParallel[T](
    collection: Iterable[T]
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

  def runInParallel[T, U](parallelism: Int, collection: Iterable[T])(
    action: T => U,
    executor: ExecutorService = Executors.newFixedThreadPool(parallelism)
  ): Iterable[U] = {
    // Create a thread pool with the given parallelism level
    implicit val ec: ExecutionContext = fromExecutor(executor)

    try {
      // Wrap each element's computation in a Future
      val futures: Iterable[Future[U]] = collection.map(item => Future(action(item)))

      // Await and return all results
      Await.result(Future.sequence(futures), Duration.Inf)
    } finally {
      // Shutdown the thread pool
      executor.shutdown()
    }
  }
}
