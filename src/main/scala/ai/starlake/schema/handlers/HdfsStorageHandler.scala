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
import ai.starlake.utils.conversion.Conversions.convertToScalaIterator
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import java.io.OutputStream
import java.nio.charset.{Charset, StandardCharsets}
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.regex.Pattern
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/** HDFS Filesystem Handler
  */
class HdfsStorageHandler(fileSystem: String)(implicit
  settings: Settings
) extends StorageHandler {

  val conf = new Configuration()
  settings.comet.hadoop.foreach { case (k, v) =>
    conf.set(k, v)
  }
  val GCSUriRegEx = "(.*)://(.*?)/(.*)".r

  private def extracSchemeAndBucketAndFilePath(uri: String): (String, Option[String], String) =
    uri match {
      case GCSUriRegEx("file", bucketName, filePath) =>
        ("file", None, "/" + filePath)
      case GCSUriRegEx(scheme, bucketName, filePath) =>
        (scheme, Some(bucketName), filePath)
      case _ => (defaultFS.getScheme, None, uri)
    }

  private def normalizedFileSystem(fileSystem: String): String = {
    if (fileSystem.endsWith(":"))
      fileSystem + "///"
    else if (!fileSystem.endsWith("://") && fileSystem.last == '/')
      fileSystem.dropRight(1)
    else if (fileSystem.endsWith("://"))
      fileSystem + "/."
    else
      fileSystem
  }

  private lazy val defaultNormalizedFileSystem = normalizedFileSystem(fileSystem)

  def lockAcquisitionPollTime: FiniteDuration = settings.comet.lock.pollTime
  def lockRefreshPollTime: FiniteDuration = settings.comet.lock.refreshTime

  conf.set("fs.defaultFS", defaultNormalizedFileSystem)

  private val defaultFS: FileSystem = FileSystem.get(conf)
  logger.info("defaultFS=" + defaultFS)
  logger.info("defaultFS.getHomeDirectory=" + defaultFS.getHomeDirectory)
  logger.info("defaultFS.getUri=" + defaultFS.getUri)

  def fs(inputPath: Path): FileSystem = {
    val (scheme, bucket, path) = extracSchemeAndBucketAndFilePath(inputPath.toString)
    val fs = defaultFS.getScheme() match {
      case "gs" =>
        conf.set("fs.defaultFS", defaultNormalizedFileSystem)
        FileSystem.get(conf)
      case _ => defaultFS
    }
    fs.setWriteChecksum(false)
    fs
  }

  /** Gets the outputstream given a path
    *
    * @param path
    *   : path
    * @return
    *   FSDataOutputStream
    */
  private def getOutputStream(path: Path): OutputStream = {
    val currentFS = fs(path)
    currentFS.delete(path, false)
    val outputStream: FSDataOutputStream = currentFS.create(path)
    outputStream
  }

  /** Read a UTF-8 text file into a string used to load yml configuration files
    *
    * @param path
    *   : Absolute file path
    * @return
    *   file content as a string
    */
  def read(path: Path, charset: Charset = StandardCharsets.UTF_8): String = {
    val currentFS = fs(path)
    val stream = currentFS.open(path)
    val content = IOUtils.toString(stream, "UTF-8")
    stream.close()
    content
  }

  /** Write a string to a UTF-8 text file. Used for yml configuration files.
    *
    * @param data
    *   file content as a string
    * @param path
    *   : Absolute file path
    */
  def write(data: String, path: Path)(implicit charset: Charset): Unit = {
    val os: FSDataOutputStream = getOutputStream(path).asInstanceOf[FSDataOutputStream]
    os.writeBytes(data)
    os.close()
  }

  /** Write bytes to binary file. Used for zip / gzip input test files.
    *
    * @param data
    *   file content as a string
    * @param path
    *   : Absolute file path
    */
  def writeBinary(data: Array[Byte], path: Path): Unit = {
    val os: OutputStream = getOutputStream(path)
    os.write(data, 0, data.length)
    os.close()
  }

  def listDirectories(path: Path): List[Path] = {
    val currentFS = fs(path)
    currentFS.listStatus(path).filter(_.isDirectory).map(_.getPath).toList
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
    * @param recursive:
    *   List all files recursively ?
    * @return
    *   List of Path
    */
  def list(
    path: Path,
    extension: String,
    since: LocalDateTime,
    recursive: Boolean,
    exclude: Option[Pattern],
    sortByName: Boolean = false
  ): List[Path] = {
    val currentFS = fs(path)
    logger.info(s"list($path, $extension, $since)")
    Try {
      if (exists(path)) {
        val iterator: RemoteIterator[LocatedFileStatus] = currentFS.listFiles(path, recursive)
        val files = iterator.filter { status =>
          logger.info(s"found file=$status")
          val time = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(status.getModificationTime),
            ZoneId.systemDefault
          )
          val excludeFile =
            exclude.exists(_.matcher(status.getPath().getName()).matches())
          !excludeFile && time.isAfter(since) && status.getPath().getName().endsWith(extension)
        }.toList

        val sorted =
          if (sortByName)
            files.sortBy(_.getPath.getName)
          else // sort by time by default
            files
              .sortBy(r => (r.getModificationTime, r.getPath.getName))
        sorted.map((status: LocatedFileStatus) => status.getPath())
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
    val currentFS = fs(src)
    delete(dest)
    mkdirs(dest.getParent)
    currentFS.rename(src, dest)
  }

  /** delete file (skip trash)
    *
    * @param path
    *   : Absolute path of file to delete
    */
  def delete(path: Path): Boolean = {
    val currentFS = fs(path)
    currentFS.delete(path, true)
  }

  /** Create folder if it does not exsit including any intermediary non existent folder
    *
    * @param path
    *   Absolute path of folder to create
    */
  def mkdirs(path: Path): Boolean = {
    val currentFS = fs(path)
    currentFS.mkdirs(path)
  }

  /** Copy file from local filesystem to target file system
    *
    * @param source
    *   Local file path
    * @param dest
    *   destination file path
    */
  def copyFromLocal(source: Path, dest: Path): Unit = {
    val currentFS = fs(source)
    currentFS.copyFromLocalFile(source, dest)
  }

  /** Copy file to local filesystem from remote file system
    *
    * @param source
    *   Remote file path
    * @param dest
    *   Local file path
    */
  def copyToLocal(source: Path, dest: Path): Unit = {
    val currentFS = fs(source)
    currentFS.copyToLocalFile(source, dest)
  }

  /** Move file from local filesystem to target file system If source FS Scheme is not "file" then
    * issue a regular move
    * @param source
    *   Local file path
    * @param dest
    *   destination file path
    */
  def moveFromLocal(source: Path, dest: Path): Unit = {
    val currentFS = fs(source)
    if (currentFS.getScheme() == "file")
      currentFS.moveFromLocalFile(source, dest)
    else
      move(source, dest)
  }

  def exists(path: Path): Boolean = {
    val currentFS = fs(path)
    currentFS.exists(path)
  }

  def blockSize(path: Path): Long = {
    val currentFS = fs(path)
    currentFS.getDefaultBlockSize(path)
  }

  def spaceConsumed(path: Path): Long = {
    val currentFS = fs(path)
    currentFS.getContentSummary(path).getSpaceConsumed
  }

  def lastModified(path: Path): Timestamp = {
    val currentFS = fs(path)
    currentFS.getFileStatus(path).getModificationTime
  }

  def touchz(path: Path): Try[Unit] = {
    val currentFS = fs(path)
    Try(currentFS.create(path, false).close())
  }

  def touch(path: Path): Try[Unit] = {
    val currentFS = fs(path)
    Try(currentFS.setTimes(path, System.currentTimeMillis(), -1))
  }

  def getScheme(): String = {
    defaultFS.getScheme
  }

}

object HdfsStorageHandler
