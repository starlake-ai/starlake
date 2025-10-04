package ai.starlake.extract

import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.ExecutionContext.fromExecutor
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object ParUtils extends LazyLogging {
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

  def runInParallel[T, U](collection: Iterable[T], parallelism: Option[Int] = None)(
    action: T => U,
    executor: Option[ExecutorService] = None,
    shutdownExecutor: Boolean = true
  ): Iterable[U] = {
    val output = withExecutor(parallelism)(
      runInExecutionContext(collection)(action)(_),
      executor = executor,
      shutdownExecutor = shutdownExecutor
    )
    output
  }

  /** If executor is provided, doesn't check minForPar and maxParOpt
    */
  def withExecutor[T](
    parallelism: Option[Int] = None
  )(
    action: Option[ExecutionContext] => T,
    executor: Option[ExecutorService] = None,
    shutdownExecutor: Boolean = true,
    minForPar: Int = 2
  ): T = {
    val (executionContext, executorService) =
      executor.map(es => Some(fromExecutor(es)) -> Some(es)).getOrElse {
        // available processor is given when None is provided as parallism
        parallelism match {
          case None =>
            val executorService1 =
              Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
            Some(fromExecutor(executorService1)) -> Some(executorService1)
          case Some(value) if value >= minForPar =>
            val executorService1 = Executors.newFixedThreadPool(value)
            Some(fromExecutor(executorService1)) -> Some(executorService1)
          case _ =>
            // don't treat as parallel if famine can occurs or user explicitly set parallism to None
            logger.info(
              s"Not enough in pool to parallelize (minimum: $minForPar). Falling back to sequential"
            )
            None -> None
        }
      }
    try {
      action(executionContext)
    } finally {
      // Shutdown the thread pool
      if (shutdownExecutor) {
        executorService.foreach(_.shutdown())
      }
    }
  }

  def runInExecutionContext[T, U](collection: Iterable[T])(
    action: T => U
  )(implicit ec: Option[ExecutionContext]): Iterable[U] = {
    ec.map { implicit executionContext =>
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
    }.getOrElse(collection.map(action))
  }

  def runOneInExecutionContext[T](
    action: T
  )(implicit ec: Option[ExecutionContext]): T = {
    runInExecutionContext(List(1))(_ => action) match {
      case el :: Nil => el
      case _         => throw new RuntimeException("Didn't expect to have more than one element")
    }
  }
}
