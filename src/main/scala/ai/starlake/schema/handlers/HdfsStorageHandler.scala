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
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import ai.starlake.utils.conversion.Conversions.convertToScalaIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import java.io.*
import java.net.URI
import java.nio.charset.{Charset, StandardCharsets}
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.regex.Pattern
import scala.util.{Failure, Success, Try, Using}

/** HDFS Filesystem Handler
  */
class HdfsStorageHandler(fileSystem: String)(implicit
  settings: Settings
) extends StorageHandler {

  private def loadGCPAuthConf(connectionOptions: Map[String, String]): Map[String, String] = {
    val authType = connectionOptions.getOrElse("authType", "APPLICATION_DEFAULT")
    val authConf = authType match {
      case "APPLICATION_DEFAULT" =>
        Map(
          "google.cloud.auth.type" -> "APPLICATION_DEFAULT"
        )
      /*
          val gcpAccessToken =
            GcpUtils.getCredentialUsingWellKnownFile().asInstanceOf[UserCredentials]

          Map(
            "google.cloud.auth.type"                   -> "USER_CREDENTIALS",
            "google.cloud.auth.service.account.enable" -> "true",
            "google.cloud.auth.client.id"              -> gcpAccessToken.getClientId,
            "google.cloud.auth.client.secret"          -> gcpAccessToken.getClientSecret,
            "google.cloud.auth.refresh.token"          -> gcpAccessToken.getRefreshToken
          )

       */
      case "SERVICE_ACCOUNT_JSON_KEYFILE" =>
        val jsonKeyFilename = connectionOptions.getOrElse(
          "jsonKeyfile",
          throw new Exception("jsonKeyfile attribute is required for SERVICE_ACCOUNT_JSON_KEYFILE")
        )

        val jsonKeyFile = BigQueryJobBase.getJsonKeyAbsoluteFile(jsonKeyFilename)
        if (!jsonKeyFile.exists()) {
          throw new Exception(s"jsonKeyfile $jsonKeyFilename does not exist")
        }

        Map(
          "google.cloud.auth.type"                         -> "SERVICE_ACCOUNT_JSON_KEYFILE",
          "google.cloud.auth.service.account.enable"       -> "true",
          "google.cloud.auth.service.account.json.keyfile" -> jsonKeyFile.toString()
        )
      case "USER_CREDENTIALS" =>
        val clientId = connectionOptions.getOrElse(
          "clientId",
          throw new Exception("clientId attribute is required for USER_CREDENTIALS")
        )
        val clientSecret = connectionOptions.getOrElse(
          "clientSecret",
          throw new Exception("clientSecret attribute is required for USER_CREDENTIALS")
        )
        val refreshToken = connectionOptions.getOrElse(
          "refreshToken",
          throw new Exception("refreshToken attribute is required for USER_CREDENTIALS")
        )
        Map(
          "google.cloud.auth.type"                   -> "USER_CREDENTIALS",
          "google.cloud.auth.service.account.enable" -> "true",
          "google.cloud.auth.client.id"              -> clientId,
          "google.cloud.auth.client.secret"          -> clientSecret,
          "google.cloud.auth.refresh.token"          -> refreshToken
        )
      case _ =>
        Map.empty[String, String]
    }
    authConf
  }

  private def loadGCPExtraConf(connectionOptions: Map[String, String]): Map[String, String] = {
    val fsConfig =
      if (settings.appConfig.fileSystem.startsWith("file:")) {
        Map.empty[String, String]
      } else {
        val bucket = connectionOptions.getOrElse(
          "gcsBucket",
          settings.appConfig.rootBucketName()
        )
        val index = bucket.indexOf("://")
        val bucketName = if (index > 0) bucket.substring(index + 3) else bucket
        val tempBucketName = connectionOptions.get("temporaryGcsBucket") match {
          case Some(tempBucket) =>
            val index = tempBucket.indexOf("://")
            if (index > 0) tempBucket.substring(index + 3) else tempBucket
          case None =>
            logger.warn(
              s"temporaryGcsBucket is not set, using $bucket as temporary bucket. " +
              s"Please set temporaryGcsBucket to a different bucket if you want to use a different one."
            )
            bucketName
        }

        // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md
        Map(
          "fs.defaultFS"                  -> bucket,
          "temporaryGcsBucket"            -> tempBucketName,
          "fs.AbstractFileSystem.gs.impl" -> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
          "fs.gs.impl"                    -> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
      }
    val authConf = loadGCPAuthConf(connectionOptions)
    fsConfig ++ authConf
  }

  private def loadAzureExtraConf(connectionOptions: Map[String, String]): Map[String, String] = {
    val azureStorageContainer = connectionOptions.getOrElse(
      "azureStorageContainer",
      throw new Exception("azureStorageContainer attribute is required for Azure Storage")
    )
    val azureStorageAccount = connectionOptions.getOrElse(
      "azureStorageAccount",
      throw new Exception("azureStorageAccount attribute is required for Azure Storage")
    )
    val azureStorageKey = connectionOptions.getOrElse(
      "azureStorageKey",
      throw new Exception("azureStorageKey attribute is required for Azure Storage")
    )
    Map(
      "fs.defaultFS" -> azureStorageContainer,
      s"fs.azure.account.auth.type.$azureStorageAccount.blob.core.windows.net" -> "SharedKey",
      s"fs.azure.account.key.$azureStorageAccount.blob.core.windows.net"       -> azureStorageKey
    )
  }

  private def loadS3ExtraConf(connectionOptions: Map[String, String]): Map[String, String] = {
    throw new Exception("S3 credentials Not yet released")
  }

  override def loadExtraConf(): Map[String, String] = {
    val options = settings.appConfig.connections
      .get(settings.appConfig.connectionRef)
      .map(_.options)
      .getOrElse(Map.empty)

    if (options.contains("authType") || options.contains("gcsBucket"))
      loadGCPExtraConf(options)
    else if (options.contains("s3Bucket"))
      loadS3ExtraConf(options)
    else if (options.contains("azureStorageContainer"))
      loadAzureExtraConf(options)
    else
      Map.empty
  }

  lazy val conf = {
    val conf = new Configuration()
    this.extraConf.foreach { case (k, v) => conf.set(k, v) }
    sys.env.get("SL_STORAGE_CONF").foreach { value =>
      value
        .split(',')
        .map { x =>
          val t = x.split('=')
          t(0).trim -> t(1).trim
        }
        .foreach { case (k, v) => conf.set(k, v) }
    }
    settings.appConfig.hadoop.foreach { case (k, v) =>
      conf.set(k, v)
    }
    conf
  }

  private def extracSchemeAndBucketAndFilePath(uri: URI): (String, Option[String], String) = {
    uri.getScheme match {
      case "file" =>
        (uri.getScheme, None, "/" + uri.getPath)
      case _ => (uri.getScheme, Option(uri.getHost), uri.getPath)
    }
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

  def lockAcquisitionPollTime: Long = settings.appConfig.lock.pollTime
  def lockRefreshPollTime: Long = settings.appConfig.lock.refreshTime

  conf.set("fs.defaultFS", defaultNormalizedFileSystem)

  private val defaultFS = FileSystem.get(conf)
  logger.info("defaultFS=" + defaultFS)
  logger.debug("defaultFS.getHomeDirectory=" + defaultFS.getHomeDirectory)
  logger.debug("defaultFS.getUri=" + defaultFS.getUri)

  def fs(inputPath: Path): FileSystem = {
    val path =
      if (inputPath.toString.contains(':')) inputPath
      else new Path(settings.appConfig.fileSystem, inputPath.toString)
    val (scheme, bucketOpt, _) = extracSchemeAndBucketAndFilePath(path.toUri)
    val fs = scheme match {
      case "gs" | "s3" | "s3a" | "s3n" =>
        bucketOpt match {
          case Some(bucket) =>
            conf.set("fs.defaultFS", normalizedFileSystem(s"$scheme://$bucket"))
            FileSystem.get(conf)
          case None =>
            throw new RuntimeException(
              "Using gs/s3 scheme must be with a bucket name. gs://bucketName"
            )
        }
      case "file" =>
        conf.set("fs.defaultFS", "file:///")
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
    pathSecurityCheck(path)
    readAndExecute(path, charset) { is =>
      org.apache.commons.io.IOUtils.toString(is)
    }
  }

  /** read input stream and do something with input
    *
    * @param path
    *   : Absolute file path
    * @return
    *   file content as a string
    */
  def readAndExecute[T](path: Path, charset: Charset = StandardCharsets.UTF_8)(
    action: InputStreamReader => T
  ): T = {
    readAndExecuteIS(path) { is =>
      val currentFS = fs(path)
      val factory = new CompressionCodecFactory(currentFS.getConf)
      val decompressedIS = Option(factory.getCodec(path)).map(_.createInputStream(is)).getOrElse(is)
      action(new InputStreamReader(decompressedIS, charset))
    }
  }

  override def readAndExecuteIS[T](path: Path)(
    action: InputStream => T
  ): T = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    Using.resource(currentFS.open(path))(action)
  }

  /** Write a string to a UTF-8 text file. Used for yml configuration files.
    *
    * @param data
    *   file content as a string
    * @param path
    *   : Absolute file path
    */
  def write(data: String, path: Path)(implicit charset: Charset): Unit = {
    pathSecurityCheck(path)
    val os: FSDataOutputStream = getOutputStream(path).asInstanceOf[FSDataOutputStream]
    os.write(data.getBytes(charset))
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
    pathSecurityCheck(path)
    val os: OutputStream = getOutputStream(path)
    os.write(data, 0, data.length)
    os.close()
  }

  def listDirectories(path: Path): List[Path] = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    currentFS
      .listStatus(path)
      .filter(_.isDirectory)
      .map(_.getPath)
      .toList
      .filterNot(_.getName.startsWith("."))
  }

  def stat(path: Path): FileInfo = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    val fileStatus = currentFS.getFileStatus(path)
    FileInfo(path, fileStatus.getLen, Instant.ofEpochMilli(fileStatus.getModificationTime))
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
    sortByName: Boolean = false
  ): List[FileInfo] = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    logger.debug(s"list($path, $extension, $since)")
    Try {
      if (exists(path)) {
        val iterator: RemoteIterator[LocatedFileStatus] = currentFS.listFiles(path, recursive)
        val files = iterator
          .filter { status =>
            logger.debug(s"found file=$status")
            val time = LocalDateTime.ofInstant(
              Instant.ofEpochMilli(status.getModificationTime),
              ZoneId.systemDefault
            )
            val excludeFile =
              exclude.exists(_.matcher(status.getPath().getName()).matches())
            !excludeFile && time.isAfter(since) && status.getPath().getName().endsWith(extension)
          }
          .toList
          .filterNot(_.getPath.getName.startsWith(".")) // ignore all control files
        logger.info(s"Found ${files.size} files")
        val sorted =
          if (sortByName)
            files.sortBy(_.getPath.getName)
          else // sort by time by default
            files
              .sortBy(r => (r.getModificationTime, r.getPath.getName))
        sorted.map((status: LocatedFileStatus) =>
          FileInfo(
            status.getPath(),
            status.getLen,
            Instant.ofEpochMilli(status.getModificationTime)
          )
        )
      } else
        Nil
    } match {
      case Success(list) => list
      case Failure(e) =>
        logger.warn(s"Ignoring folder $path", e)
        Nil
    }
  }

  /** Copy file
    *
    * @param src
    *   source path
    * @param dst
    *   destination path
    * @return
    */
  override def copy(src: Path, dst: Path): Boolean = {
    pathSecurityCheck(src)
    pathSecurityCheck(dst)
    FileUtil.copy(fs(src), src, fs(dst), dst, false, conf)
  }

  /** Move file
    *
    * @param src
    *   source path (file or folder)
    * @param dest
    *   destination path (file or folder)
    * @return
    */
  def move(src: Path, dst: Path): Boolean = {
    pathSecurityCheck(src)
    val destFS = fs(dst)
    val sourceFS = fs(src)
    val destURI = destFS.getUri
    val sourceURI = sourceFS.getUri
    if (sourceURI.getScheme == destURI.getScheme && sourceURI.getHost == destURI.getHost) {
      delete(dst)
      mkdirs(dst.getParent)
      sourceFS.rename(src, dst)
    } else {
      delete(dst)
      dst.getParent.toString.split("://") match {
        case Array(_, path) =>
          if (path.split("/").count(_.nonEmpty) > 1)
            mkdirs(dst.getParent)
        case _ =>
          mkdirs(dst.getParent) // local file system
      }
      FileUtil.copy(fs(src), src, fs(dst), dst, true, conf)
    }
  }

  /** delete file (skip trash)
    *
    * @param path
    *   : Absolute path of file to delete
    */
  def delete(path: Path): Boolean = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    currentFS.delete(path, true) // recursive delete
  }

  /** Create folder if it does not exist including any intermediary non-existent folder
    *
    * @param path
    *   Absolute path of folder to create
    */
  def mkdirs(path: Path): Boolean = {
    pathSecurityCheck(path)
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
    pathSecurityCheck(source)
    pathSecurityCheck(dest)
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
    pathSecurityCheck(source)
    pathSecurityCheck(dest)
    val currentFS = fs(source)
    currentFS.copyToLocalFile(source, dest)
  }

  /** Move file from local filesystem to target file system If source FS Scheme is not "file" then
    * issue a regular move
    *
    * @param source
    *   Local file path
    * @param dest
    *   destination file path
    */
  def moveFromLocal(source: Path, dest: Path): Unit = {
    pathSecurityCheck(source)
    pathSecurityCheck(dest)
    val currentFS = fs(source)
    if (currentFS.getScheme() == "file")
      currentFS.moveFromLocalFile(source, dest)
    else
      move(source, dest)
  }

  def exists(path: Path): Boolean = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    currentFS.exists(path)
  }

  def blockSize(path: Path): Long = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    currentFS.getDefaultBlockSize(path)
  }

  def spaceConsumed(path: Path): Long = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    currentFS.getContentSummary(path).getSpaceConsumed
  }

  def lastModified(path: Path): Timestamp = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    currentFS.getFileStatus(path).getModificationTime
  }

  def touchz(path: Path): Try[Unit] = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    Try(currentFS.create(path, false).close())
  }

  def touch(path: Path): Try[Unit] = {
    pathSecurityCheck(path)
    val currentFS = fs(path)
    Try(currentFS.setTimes(path, System.currentTimeMillis(), -1))
  }

  def getScheme(): String = {
    defaultFS.getScheme
  }

  def copyMerge(
    header: Option[String],
    srcDir: Path,
    dstFile: Path,
    deleteSource: Boolean
  ): Boolean = {
    val currentFS = fs(dstFile)

    if (currentFS.exists(dstFile)) {
      throw new IOException(s"Target $dstFile already exists")
    }

    // Source path is expected to be a directory:
    if (currentFS.getFileStatus(srcDir).isDirectory) {
      val parts = currentFS
        .listStatus(srcDir)
        .filter(status => status.isFile)
        .map(_.getPath)
      if (parts.nonEmpty || header.nonEmpty) {
        val outputStream = currentFS.create(dstFile)
        header.foreach { header =>
          val headerWithNL = if (header.endsWith("\n")) header else header + "\n"
          val inputStream = new ByteArrayInputStream(headerWithNL.getBytes)
          try { org.apache.hadoop.io.IOUtils.copyBytes(inputStream, outputStream, conf, false) }
          finally { inputStream.close() }
        }
        try {
          parts
            .filter(part => part.getName().startsWith("part-"))
            .sortBy(_.getName)
            .collect { case part =>
              val inputStream = currentFS.open(part)
              try {
                org.apache.hadoop.io.IOUtils.copyBytes(inputStream, outputStream, conf, false)
                if (deleteSource) delete(part)
              } finally { inputStream.close() }
            }

        } finally { outputStream.close() }
      }
      true
    } else {
      false
    }
  }

  override def open(path: Path): Option[InputStream] = {
    pathSecurityCheck(path)
    Try(fs(path).open(path)) match {
      case Success(is) => Some(is)
      case Failure(f) =>
        logger.error(f.getMessage)
        None
    }
  }

  override def output(path: Path): OutputStream = getOutputStream(path)
}

object HdfsStorageHandler
