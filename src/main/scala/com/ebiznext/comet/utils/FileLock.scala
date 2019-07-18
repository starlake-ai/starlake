package com.ebiznext.comet.utils

import com.ebiznext.comet.schema.handlers.StorageHandler
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success}

class FileLock(path: Path, storageHandler: StorageHandler) extends StrictLogging {
  val checkinPeriod = 5000L
  val fileWatcher = new LockWatcher(path, storageHandler, checkinPeriod)

  def tryLock(timeoutInMillis: Long): Boolean = {
    storageHandler.mkdirs(path.getParent)
    val maxTries = timeoutInMillis / checkinPeriod
    var numberOfTries = 1
    logger.info(s"Trying to acquire lock for file ${path.toString} during $timeoutInMillis ms")
    var ok = false
    while (numberOfTries <= maxTries && !ok) {
      ok = storageHandler.touchz(path) match {
        case Success(_) =>
          logger.info(
            s"Succeeded to acquire lock for file ${path.toString} after $numberOfTries tries"
          )
          watch()
          true
        case Failure(_) =>
          val lastModified = storageHandler.lastModified(path)
          logger.info(s"""
              |lastModified=$lastModified
              |System.currentTimeMillis()=${System.currentTimeMillis()}
              |checkinPeriod*4=${checkinPeriod * 4}

          """)
          if (System.currentTimeMillis() - lastModified > checkinPeriod * 4) {
            storageHandler.delete(path)
          }
          numberOfTries = numberOfTries + 1
          Thread.sleep(checkinPeriod)
          false
      }
    }
    ok
  }

  def release(): Unit = fileWatcher.release()

  private def watch(): Unit = {
    val th = new Thread(fileWatcher, s"LockWatcher-${System.currentTimeMillis()}")
    th.start()
  }
}

class LockWatcher(path: Path, storageHandler: StorageHandler, checkinPeriod: Long)
    extends Runnable {

  def release(): Unit =
    stop = true

  var stop = false
  override def run(): Unit = {
    try {
      while (!stop) {
        Thread.sleep(checkinPeriod)
        storageHandler.touch(path)
        println(s"watcher modified=${storageHandler.lastModified(path)}")
      }
    } catch {
      case e: InterruptedException =>
        e.printStackTrace();
    }
  }

}
