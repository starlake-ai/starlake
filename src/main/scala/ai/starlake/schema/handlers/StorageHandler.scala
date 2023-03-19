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

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import java.nio.charset.Charset.defaultCharset
import java.nio.charset.{Charset, StandardCharsets}
import java.time.LocalDateTime
import java.util.regex.Pattern
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/** Interface required by any filesystem manager
  */
trait StorageHandler extends StrictLogging {

  def move(src: Path, dst: Path): Boolean

  def delete(path: Path): Boolean

  def exists(path: Path): Boolean

  def mkdirs(path: Path): Boolean

  def copyFromLocal(source: Path, dest: Path): Unit

  def copyToLocal(source: Path, dest: Path): Unit

  def moveFromLocal(source: Path, dest: Path): Unit

  def moveSparkPartFile(sparkFolder: Path, extension: String): Option[Path] = {
    val files = list(sparkFolder, extension = extension, recursive = false).headOption
    files.map { f =>
      val tmpFile = new Path(sparkFolder.getParent, sparkFolder.getName + ".tmp")
      move(f, tmpFile)
      delete(sparkFolder)
      move(tmpFile, sparkFolder)
      sparkFolder
    }
  }

  def read(path: Path, charset: Charset = StandardCharsets.UTF_8): String

  def write(data: String, path: Path)(implicit charset: Charset = defaultCharset): Unit

  def writeBinary(data: Array[Byte], path: Path): Unit

  def listDirectories(path: Path): List[Path]

  def list(
    path: Path,
    extension: String = "",
    since: LocalDateTime = LocalDateTime.MIN,
    recursive: Boolean,
    exclude: Option[Pattern] = None,
    sortByName: Boolean = false // sort by time by default
  ): List[Path]

  def blockSize(path: Path): Long

  def lastModified(path: Path): Timestamp

  def spaceConsumed(path: Path): Long

  def touchz(path: Path): Try[Unit]

  def touch(path: Path): Try[Unit]

  def lockAcquisitionPollTime: FiniteDuration
  def lockRefreshPollTime: FiniteDuration

  def getScheme(): String
}
