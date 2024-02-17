package ai.starlake.job.load

import ai.starlake.schema.handlers.{FileInfo, StorageHandler}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import java.time.LocalDateTime

object IngestionNameStrategy extends LoadStrategy with StrictLogging {

  def list(
    storageHandler: StorageHandler,
    path: Path,
    extension: String = "",
    since: LocalDateTime = LocalDateTime.MIN,
    recursive: Boolean
  ): List[FileInfo] =
    storageHandler.list(path, extension, since, recursive, None, sortByName = true)
}
