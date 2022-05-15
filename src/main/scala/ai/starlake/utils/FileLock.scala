package ai.starlake.utils

import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.handlers.StorageHandler
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Semaphore, TimeUnit, TimeoutException}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/** HDFS does not have a file locking mechanism. We implement it here in the following way
  *   - A file is locked when it's modification time is less than 5000ms than the current time
  *   - If the modification time is older tahn the current time of more than 5000ms, it is
  *     considered unlocked.
  *   - The owner of a lock spawn a thread that update the modification every 5s to keep the lock
  *   - The process willing to get the lock check every 5s second for the modification time of the
  *     file to gain the lock.
  *   - When a process gain the lock, it deletes the file first and tries to create a new one, this
  *     make sure that of two process gaining the lock, only one will be able to recreate it after
  *     deletion.
  *
  * To gain the lock, call [[tryLock]], to release it, call `release()`. If you are going to use the
  * lock and release it within the same call stack frame, please consider calling `tryExclusively()`
  * (as in exclusive(delay) { action }`) instead.
  *
  * @param path
  *   Lock File path
  * @param storageHandler
  *   Filesystem Handler
  */
class FileLock(path: Path, storageHandler: StorageHandler) extends StrictLogging {
  def checkinPeriod: Long = storageHandler.lockAcquisitionPollTime.toMillis
  def refreshPeriod: Long = storageHandler.lockRefreshPollTime.toMillis

  private val fileWatcher = new FileLock.LockWatcher(path, storageHandler, refreshPeriod)

  /** Try to perform an operation while holding a lock exclusively
    * @param timeoutInMillis
    *   number of milliseconds during which the calling process will try to get the lock before it
    *   time out. -1 means no timeout
    * @return
    *   the result of the operation (if successful) when locked is acquired, Failure otherwise
    *
    * the lock is guaranteed freed upon normal or exceptional exit from this function.
    */
  def tryExclusively[T](timeoutInMillis: Long = -1L)(op: => T): Try[T] = {
    Try(doExclusively(timeoutInMillis)(op))
  }

  /** Try to perform an operation while holding a lock exclusively
    * @param timeoutInMillis
    *   number of milliseconds during which the calling process will try to get the lock before it
    *   times out. -1 means no timeout
    * @return
    *   the result of the operation (if successful) when locked is acquired throws TimeoutException
    *   if the lock could not be acquired within timeoutInMillis, or any exception thrown by op if
    *   the lock was acquired but the operation failed
    *
    * the lock is guaranteed freed upon normal or exceptional exit from this function.
    */
  def doExclusively[T](timeoutInMillis: Long = -1L)(op: => T): T = {
    if (tryLock(timeoutInMillis)) {
      try {
        op
      } finally {
        release()
      }
    } else {
      throw new TimeoutException(
        s"Failed to obtain lock on file $path waited (millis) $timeoutInMillis"
      )
    }
  }

  /** Try to gain the lock during timeoutInMillis millis
    *
    * @param timeoutInMillis
    *   number of milliseconds during which the calling process will try to get the lock before it
    *   time out. -1 means no timeout
    * @return
    *   true when locked is acquired (caller is responsible for calling `release()`, false otherwise
    */
  def tryLock(timeoutInMillis: Long = -1): Boolean = {
    fileWatcher.checkPristine()

    storageHandler.mkdirs(path.getParent)
    val maxTries =
      if (timeoutInMillis == -1) Integer.MAX_VALUE else (timeoutInMillis / checkinPeriod).toInt
    logger.info(s"Trying to acquire lock for file ${path.toString} during $timeoutInMillis ms")

    @tailrec
    def getLock(numberOfTries: Int): Boolean = {
      if (numberOfTries == 0)
        false
      else {
        storageHandler.touchz(path) match {
          case Success(_) =>
            logger.info(
              s"Succeeded to acquire lock for file ${path.toString} after $numberOfTries tries"
            )
            watch()
            true
          case Failure(e) =>
            logger.info(s"Audit lock ${path.toString} already in use waiting ...  ${e.getMessage}")
            Try {
              storageHandler.lastModified(path)
            } match {
              case Success(lastModified) =>
                val currentTimeMillis = System.currentTimeMillis()
                logger.info(s"""
                               |lastModified=$lastModified
                               |System.currentTimeMillis()=${currentTimeMillis}
                               |checkinPeriod*4=${checkinPeriod * 4}
                               |refreshPeriod*4=${refreshPeriod * 4}
                               |""".stripMargin)
                if (currentTimeMillis - lastModified > refreshPeriod * 4) {
                  storageHandler.delete(path)
                }

              case Failure(e) =>
                logger.info(
                  s"${path.toString} was deleted during access to modification date ${e.getMessage}"
                )
            }
            Thread.sleep(checkinPeriod)
            getLock(numberOfTries - 1)
        }
      }
    }
    getLock(maxTries)
  }

  /** Release the lock and delete the lock file.
    */
  def release(): Unit = fileWatcher.release()

  private def watch(): Unit = {
    val th = new Thread(fileWatcher, s"LockWatcher-${System.currentTimeMillis()}-${path.toString}")
    th.start()
  }
}

object FileLock {

  private class LockWatcher(path: Path, storageHandler: StorageHandler, reportingPeriod: Long)
      extends Runnable
      with StrictLogging {

    private val pristine = new AtomicBoolean(true)

    def checkPristine(): Unit = {
      val wasPristine = pristine.getAndSet(false)
      if (!wasPristine)
        throw new IllegalStateException(
          s"FileLock instance on ${path} had already been used, cannot re-use"
        )
    }
    private val spent = new AtomicBoolean(false)

    private val sem = new Semaphore(0)

    def release(): Unit = {
      val wasAlreadySpent = spent.getAndSet(true)
      if (wasAlreadySpent)
        throw new IllegalStateException(
          s"LockWatcher thread on ${path} already spent, cannot release again"
        )
      sem.release()
    }

    override def run(): Unit = {
      /* if this thread starts, this means that our parent thread owns the lock.
       *
       * Our purpose is to regularly remind the user that something on *this* JVM owns the lock, and then destroy the
       * lockfile once we've been notified we no longer need to.
       *
       * We also regularly touch the lockfile in order to demonstrate to external users (on other JVMs or on other nodes)
       * that indeed, something is still alive and interested in this lock. An operator might use this as a clue to
       * decide that a lock file is stale and deserves to be removed.
       * */
      try {
        while (!sem.tryAcquire(reportingPeriod, TimeUnit.MILLISECONDS)) {
          // we've slept checkinPeriod and failed to acquire the semaphore; let's remind the operator that we hold the lock, and carry on
          storageHandler.touch(path)
          logger.info(s"watcher $path modified=${storageHandler.lastModified(path)}")
        }
        storageHandler.delete(path)
      } catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
    }

  }
}
