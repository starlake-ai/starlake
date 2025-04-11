package ai.starlake.extract

import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.ExecutionContext.fromExecutor
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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
      val futures: Iterable[Future[U]] = collection.map(item => Future(action(item)))

      def lift[T](futures: Iterable[Future[T]]): Iterable[Future[Try[T]]] =
        futures.map(_.map { Success(_) }.recover { case t => Failure(t) })

      def waitAll[T](futures: Iterable[Future[T]]): Future[Iterable[Try[T]]] =
        Future.sequence(
          lift(futures)
        )

      // Waiting for just Future.sequence without wrapping into a Try makes
      // exception thrown to shutdown executorService prematuraly.
      // This allows other futures to be scheduled.
      Await.result(waitAll(futures), Duration.Inf).map {
        case Success(e)         => e
        case Failure(exception) => throw exception
      }
    } finally {
      // Shutdown the thread pool
      executor.shutdown()
    }
  }
}
