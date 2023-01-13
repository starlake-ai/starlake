/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.schema.handlers

import ai.starlake.config.Settings
import better.files.File
import org.apache.commons.lang.SystemUtils
import org.apache.hadoop.fs._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import java.io.OutputStream
import java.nio.charset.{Charset, StandardCharsets}
import java.time.{LocalDateTime, ZoneId}
import java.util.regex.Pattern
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/** HDFS Filesystem Handler
  */
class LocalStorageHandler(implicit
  settings: Settings
) extends StorageHandler {

  import LocalStorageHandler._

  def lockAcquisitionPollTime: FiniteDuration = settings.comet.lock.pollTime

  def lockRefreshPollTime: FiniteDuration = settings.comet.lock.refreshTime

  /** Gets the outputstream given a path
    *
    * @param path
    *   : path
    * @return
    *   FSDataOutputStream
    */
  def getOutputStream(path: Path): OutputStream = {
    val file = localFile(path)
    file.delete(true)
    file.newOutputStream()
  }

  /** Read a UTF-8 text file into a string used to load yml configuration files
    *
    * @param path
    *   : Absolute file path
    * @return
    *   file content as a string
    */
  def read(path: Path, charset: Charset = StandardCharsets.UTF_8): String = {
    val file = localFile(path)
    file.contentAsString(charset)
  }

  /** Write a string to a UTF-8 text file. Used for yml configuration files.
    *
    * @param data
    *   file content as a string
    * @param path
    *   : Absolute file path
    */
  def write(data: String, path: Path)(implicit charset: Charset): Unit = {
    val file = localFile(path)
    file.parent.createDirectories()
    file.overwrite(data)
  }

  /** Write bytes to binary file. Used for zip / gzip input test files.
    *
    * @param data
    *   file content as a string
    * @param path
    *   : Absolute file path
    */
  def writeBinary(data: Array[Byte], path: Path): Unit = {
    val file = localFile(path)
    file.parent.createDirectories()
    file.writeByteArray(data)
  }

  def listDirectories(path: Path): List[Path] = {
    val file = localFile(path)
    file.list.filter(_.isDirectory).map(f => new Path(f.pathAsString)).toList
  }

  /** List all files in folder
    *
    * @param path
    *   Absolute folder path
    * @param extension
    *   : Files should end with this string. To list all files, simply provide an empty string
    * @param since
    *   Minimum modification time of liste files. To list all files, simply provide the beginning of
    *   all times
    * @param recursive
    *   : List all files recursively ?
    * @return
    *   List of Path
    */
  def list(
    path: Path,
    extension: String,
    since: LocalDateTime,
    recursive: Boolean,
    exclude: Option[Pattern],
    sortByName: Boolean = false // sort by time by default
  ): List[Path] = {
    logger.info(s"list($path, $extension, $since)")
    Try {
      if (exists(path)) {
        val file = localFile(path)
        val fileList =
          if (recursive)
            file.listRecursively()
          else
            file.list

        val iterator = fileList.filter(_.isRegularFile)
        val files = iterator.filter { f =>
          logger.info(s"found file=$f")
          val time = LocalDateTime.ofInstant(f.lastModifiedTime, ZoneId.systemDefault)
          val excludeFile =
            exclude.exists(_.matcher(f.name).matches())
          !excludeFile && time.isAfter(since) && f.name.endsWith(extension)
        }.toList
        val sorted =
          if (sortByName)
            files.sortBy(_.name)
          else
            files.sortBy(f => (f.lastModifiedTime, f.name))

        sorted.map(f => {
          new Path(f.pathAsString)
        })
      } else
        Nil
    } match {
      case Success(list) => list
      case Failure(e) =>
        logger.warn(s"Ignoring folder $path", e)
        Nil
    }
  }

  /** Move file
    *
    * @param path
    *   source path (file or folder)
    * @param dest
    *   destination path (file or folder)
    * @return
    */
  def move(src: Path, dest: Path): Boolean = {
    val fsrc = localFile(src)
    val fdest = localFile(dest)
    fdest.delete(true)
    mkdirs(dest.getParent)
    fsrc.moveTo(fdest)
    true
  }

  /** delete file (skip trash)
    *
    * @param path
    *   : Absolute path of file to delete
    */
  def delete(path: Path): Boolean = {
    val file = localFile(path)
    file.delete(true)
    true
  }

  /** Create folder if it does not exsit including any intermediary non existent folder
    *
    * @param path
    *   Absolute path of folder to create
    */
  def mkdirs(path: Path): Boolean = {
    val file = localFile(path)
    file.createDirectories()
    true
  }

  /** Copy file from local filesystem to target file system
    *
    * @param source
    *   Local file path
    * @param dest
    *   destination file path
    */
  def copyFromLocal(src: Path, dest: Path): Unit = {
    val fsrc = localFile(src)
    val fdest = localFile(dest)
    fdest.delete(true)
    mkdirs(dest.getParent)
    fsrc.copyTo(fdest)

  }

  /** Copy file to local filesystem from remote file system
    *
    * @param source
    *   Remote file path
    * @param dest
    *   Local file path
    */
  def copyToLocal(src: Path, dest: Path): Unit = copyFromLocal(src, dest)

  /** Move file from local filesystem to target file system If source FS Scheme is not "file" then
    * issue a regular move
    *
    * @param source
    *   Local file path
    * @param dest
    *   destination file path
    */
  def moveFromLocal(source: Path, dest: Path): Unit = {
    this.move(source, dest)
  }

  def exists(path: Path): Boolean = {
    val file = localFile(path)
    file.exists
  }

  def blockSize(path: Path): Long = {
    64 * 1024 * 1024 // 64 MB
  }

  def spaceConsumed(path: Path): Long = {
    val file = localFile(path)
    file.size()
  }

  def lastModified(path: Path): Timestamp = {
    val file = localFile(path)
    file.lastModifiedTime.toEpochMilli
  }

  def touchz(path: Path): Try[Unit] = {
    val file = localFile(path)
    Try(file.touch())
  }

  def touch(path: Path): Try[Unit] = {
    touchz(path)
  }

  def getScheme(): String = "file"

}

object LocalStorageHandler {
  private val HAS_DRIVE_LETTER_SPECIFIER = Pattern.compile("^/?[a-zA-Z]:")

  def localFile(path: Path): File = {
    val pathAsString = path.toUri.getPath
    val isWindowsFile =
      SystemUtils.IS_OS_WINDOWS && HAS_DRIVE_LETTER_SPECIFIER.matcher(pathAsString).find()
    if (isWindowsFile)
      File(pathAsString.substring(1))
    else
      File(pathAsString)
  }
}
