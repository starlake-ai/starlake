package com.ebiznext.comet.job.load

import java.time.{Instant, LocalDateTime, ZoneId}

import com.ebiznext.comet.utils.conversion.Conversions.convertToScalaIterator
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

import scala.util.{Failure, Success, Try}

object IngestionTimeStrategy extends LoadStrategy with StrictLogging {

  def list(
    fs: FileSystem,
    path: Path,
    extension: String = "",
    since: LocalDateTime = LocalDateTime.MIN,
    recursive: Boolean
  ): List[Path] = {
    Try {
      val iterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, recursive)
      iterator
        .filter { status =>
          logger.info(s"found file=$status")
          val time = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(status.getModificationTime),
            ZoneId.systemDefault
          )
          time.isAfter(since) && status.getPath.getName.endsWith(extension)
        }
        .toList
        .sortBy(p => (p.getModificationTime, p.getPath.getName))
        .map((status: LocatedFileStatus) => status.getPath)
    } match {
      case Success(list) => list
      case Failure(e) =>
        logger.warn(s"Ignoring folder $path", e)
        Nil
    }
  }
}
