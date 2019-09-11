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

package com.ebiznext.comet.schema.handlers

import java.io.ByteArrayOutputStream
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import scala.util.Try

/**
  * Interface required by any filesystem manager
  */
trait StorageHandler {

  def move(src: Path, dst: Path): Boolean

  def delete(path: Path): Boolean

  def exists(path: Path): Boolean

  def mkdirs(path: Path): Boolean

  def copyFromLocal(source: Path, dest: Path): Unit

  def moveFromLocal(source: Path, dest: Path): Unit

  def read(path: Path): String

  def write(data: String, path: Path): Unit

  def list(path: Path, extension: String = "", since: LocalDateTime = LocalDateTime.MIN): List[Path]

  def blockSize(path: Path): Long

  def contentSummary(path: Path): ContentSummary

  def lastModified(path: Path): Timestamp

  def spaceConsumed(path: Path): Long

  def getOutputStream(path: Path): FSDataOutputStream

  def touchz(path: Path): Try[Unit]

  def touch(path: Path): Try[Unit]

  def unzip(source: Path, targetDir: Path): Try[Unit]

}

/**
  * HDFS Filesystem Handler
  */
class HdfsStorageHandler(fileSystem: Option[String]) extends StorageHandler {

  val conf = new Configuration()
  fileSystem.map(fileSystem => conf.set("fs.defaultFS", fileSystem))
  val fs: FileSystem = FileSystem.get(conf)

  /**
    * Gets the outputstream given a path
    *
    * @param path : path
    * @return FSDataOutputStream
    */
  def getOutputStream(path: Path): FSDataOutputStream = {
    fs.delete(path, false)
    val outputStream: FSDataOutputStream = fs.create(path)
    outputStream
  }

  /**
    * Read a UTF-8 text file into a string used to load yml configuration files
    *
    * @param path : Absolute file path
    * @return file content as a string
    */
  def read(path: Path): String = {

    val stream = fs.open(path)
    val content = IOUtils.toString(stream, "UTF-8")
    content
  }

  /**
    * Write a string to a UTF-8 text file. Used for yml configuration files.
    *
    * @param data file content as a string
    * @param path : Absolute file path
    */
  def write(data: String, path: Path): Unit = {
    val os: FSDataOutputStream = getOutputStream(path)
    os.writeBytes(data)
    os.close()
  }

  /**
    * List all files in folder
    *
    * @param path      Absolute folder path
    * @param extension : Files should end with this string. To list all files, simply provide an empty string
    * @param since     Minimum modification time of liste files. To list all files, simply provide the beginning of all times
    * @return List of Path
    */
  def list(path: Path, extension: String, since: LocalDateTime): List[Path] = {

    val iterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, false)
    iterator
      .filter { status =>
        val time = LocalDateTime.ofInstant(
          Instant.ofEpochMilli(status.getModificationTime),
          ZoneId.systemDefault
        )
        time.isAfter(since) && status.getPath().getName().endsWith(extension)
      }
      .map(status => status.getPath())
      .toList
  }

  /**
    * Move file
    *
    * @param path source path (file or folder)
    * @param dest destination path (file or folder)
    * @return
    */
  override def move(path: Path, dest: Path): Boolean = {

    FileUtil.copy(fs, path, fs, dest, true, true, conf)
  }

  /**
    * delete file (skip trash)
    *
    * @param path : Absolute path of file to delete
    */
  override def delete(path: Path): Boolean = {

    fs.delete(path, true)
  }

  /**
    * Create folder if it does not exsit including any intermediary non existent folder
    *
    * @param path Absolute path of folder to create
    */
  override def mkdirs(path: Path): Boolean = {

    fs.mkdirs(path)
  }

  /**
    * Copy file from local filesystem to target file system
    *
    * @param source Local file path
    * @param dest   destination file path
    */
  override def copyFromLocal(source: Path, dest: Path): Unit = {

    fs.copyFromLocalFile(source, dest)
  }

  /**
    * Move file from local filesystem to target file system
    *
    * @param source Local file path
    * @param dest   destination file path
    */
  override def moveFromLocal(source: Path, dest: Path): Unit = {

    fs.moveFromLocalFile(source, dest)
  }

  override def exists(path: Path): Boolean = {

    fs.exists(path)
  }

  def blockSize(path: Path): Long = {

    fs.getDefaultBlockSize(path)
  }

  def contentSummary(path: Path): ContentSummary = {

    fs.getContentSummary(path)
  }

  def spaceConsumed(path: Path): Long = {
    contentSummary(path).getSpaceConsumed
  }

  def lastModified(path: Path): Timestamp = {

    fs.getFileStatus(path).getModificationTime
  }

  override def touchz(path: Path): Try[Unit] = {

    Try(fs.create(path, false).close())
  }

  override def touch(path: Path): Try[Unit] = {

    Try(fs.setTimes(path, System.currentTimeMillis(), -1))

  }

  override def unzip(sourceFile: Path, targetDir: Path): Try[Unit] = {
    import java.util.zip.ZipInputStream
    Try {
      if (!fs.exists(sourceFile)) throw new Exception(sourceFile.toString + " does not exist")
      val fsInputStream = fs.open(sourceFile)
      val zipInputStream = new ZipInputStream(fsInputStream)
      Stream
        .continually(zipInputStream.getNextEntry)
        .takeWhile(ze => ze != null && !ze.isDirectory)
        .foreach { zipEntry =>
          val entryName = zipEntry.getName()
          val outputStream = new ByteArrayOutputStream()
          val buf = new Array[Byte](4096)
          var bytesRead = zipInputStream.read(buf, 0, 4096)
          while (bytesRead > -1) {
            outputStream.write(buf, 0, bytesRead)
            bytesRead = zipInputStream.read(buf, 0, 4096)
          }
          outputStream.close()
          zipInputStream.closeEntry()
        }
      zipInputStream.close()

    }
  }
}
