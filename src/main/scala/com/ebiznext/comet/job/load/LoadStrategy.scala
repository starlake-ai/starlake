package com.ebiznext.comet.job.load

import java.time.LocalDateTime

import org.apache.hadoop.fs.{FileSystem, Path}

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
    fs: FileSystem,
    path: Path,
    extension: String = "",
    since: LocalDateTime = LocalDateTime.MIN,
    recursive: Boolean
  ): List[Path]
}
