package ai.starlake.job.load

import ai.starlake.schema.handlers.StorageHandler
import org.apache.hadoop.fs.Path

import java.time.LocalDateTime

trait LoadStrategy {

  /** List all files in folder
    *
    * @param fs
    *   FileSystem
    * @param path
    *   Absolute folder path
    * @param extension
    *   Files should end with this string. To list all files, simply provide an empty string
    * @param since
    *   Minimum modification time of list files. To list all files, simply provide the beginning of
    *   all times
    * @param recursive
    *   List files recursively
    * @return
    *   List of Path
    */
  def list(
    storageHandler: StorageHandler,
    path: Path,
    extension: String = "",
    since: LocalDateTime = LocalDateTime.MIN,
    recursive: Boolean
  ): List[Path]

}
