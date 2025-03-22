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

package ai.starlake.workflow

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.ParUtils
import ai.starlake.job.infer.{InferSchemaConfig, InferSchemaJob}
import ai.starlake.job.ingest._
import ai.starlake.job.load.LoadStrategy
import ai.starlake.job.metrics.{MetricsConfig, MetricsJob}
import ai.starlake.job.sink.bigquery.{
  BigQueryJobBase,
  BigQueryJobResult,
  BigQueryLoadConfig,
  BigQuerySparkJob
}
import ai.starlake.job.sink.es.{ESLoadConfig, ESLoadJob}
import ai.starlake.job.sink.jdbc.{JdbcConnectionLoadConfig, SparkJdbcWriter}
import ai.starlake.job.sink.kafka.{KafkaJob, KafkaJobConfig}
import ai.starlake.job.transform.{AutoTask, TransformConfig}
import ai.starlake.lineage.{
  AutoTaskDependencies,
  AutoTaskDependenciesConfig,
  TaskViewDependencyNode
}
import ai.starlake.schema.AdaptiveWriteStrategy
import ai.starlake.schema.generator._
import ai.starlake.schema.handlers.{FileInfo, SchemaHandler, StorageHandler}
import ai.starlake.schema.model.Engine.BQ
import ai.starlake.schema.model.Mode.{FILE, STREAM}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.tests.{
  StarlakeTestConfig,
  StarlakeTestCoverage,
  StarlakeTestData,
  StarlakeTestResult
}
import ai.starlake.utils._
import better.files.File
import com.manticore.jsqlformatter.JSQLFormatter
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import java.nio.file.{FileSystems, ProviderNotFoundException}
import java.time.Instant
import java.util.Collections
import scala.annotation.nowarn
import scala.reflect.io.Directory
import scala.util.{Failure, Success, Try}

/** The whole worklfow works as follow :
  *   - loadLanding : Zipped files are uncompressed or raw files extracted from the local
  *     filesystem.
  * -loadPending : files recognized with filename patterns are stored in the ingesting area and
  * submitted for ingestion files with unrecognized filename patterns are stored in the unresolved
  * area
  *   - ingest : files are finally ingested and saved as parquet/orc/... files and hive tables
  *
  * @param storageHandler
  *   : Minimum set of features required for the underlying filesystem
  * @param schemaHandler
  *   : Schema interface
  */
class IngestionWorkflow(
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler
)(implicit settings: Settings)
    extends StrictLogging {
  private var _domains: Option[List[Domain]] = None

  private def domains(
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil
  ): List[Domain] = {
    _domains match {
      case None =>
        if (domainNames.nonEmpty)
          logger.info(s"Loading domains : ${domainNames.mkString(",")}")
        else
          logger.info("Loading all domains")
        if (tableNames.nonEmpty)
          logger.info(s"Loading tables : ${tableNames.mkString(",")}")
        else
          logger.info("Loading all tables")
        val domains = schemaHandler.domains(domainNames, tableNames)
        _domains = Some(domains)
        domains
      case Some(domains) =>
        logger.info("Domains already loaded")
        domains
    }
  }

  /** Find all the domains that have a filename pattern that matches the given filename
    *
    * @param filename
    *   : filename to match
    * @return
    *   list of domain name and table name
    */
  def findAllFilenameMatchers(
    filename: String,
    domain: Option[String],
    table: Option[String]
  ): List[(String, String)] = {
    (domain, table) match {
      case (Some(domain), Some(table)) =>
        val path = new Path(new Path(DatasetArea.load, domain), table + ".sl.yml")
        val exists = settings.storageHandler().exists(path)
        if (exists) {
          val tableContent = settings
            .storageHandler()
            .read(path)
          val rootNode = YamlSerde.deserializeYamlTables(tableContent, path.toString).head
          if (rootNode.table.pattern.matcher(filename).matches()) {
            List((domain, table))
          } else {
            Nil
          }
        } else {
          Nil
        }
      case (Some(domain), None) =>
        val domains = schemaHandler.domains(List(domain), reload = true)
        domains.flatMap { domain =>
          domain.tables.flatMap { table =>
            val pattern = table.pattern
            if (pattern.matcher(filename).matches()) {
              Some((domain.name, table.name))
            } else {
              None
            }
          }
        }
      case (_, _) =>
        val domains = schemaHandler.domains(reload = true)
        domains.flatMap { domain =>
          domain.tables.flatMap { table =>
            val pattern = table.pattern
            if (pattern.matcher(filename).matches()) {
              Some((domain.name, table.name))
            } else {
              None
            }
          }
        }
    }
  }

  /** Move the files from the landing area to the pending area. files are loaded one domain at a
    * time each domain has its own directory and is specified in the "directory" key of Domain YML
    * file compressed files are uncompressed if a corresponding ack file exist. Compressed files are
    * recognized by their extension which should be one of .tgz, .zip, .gz. raw file should also
    * have a corresponding ack file before moving the files to the pending area, the ack files are
    * deleted To import files without ack specify an empty "ack" key (aka ack:"") in the domain YML
    * file. "ack" is the default ack extension searched for but you may specify a different one in
    * the domain YML file.
    */

  def stage(config: StageConfig): Try[Unit] = Try {
    val filteredDomains = config.domains match {
      case Nil => domains(Nil, Nil) // Load all domains & tables
      case _   => domains(config.domains.toList, config.tables.toList)
    }
    logger.info(
      s"Loading files from Landing Zone for domains : ${filteredDomains.map(_.name).mkString(",")}"
    )
    filteredDomains.foreach { domain =>
      val storageHandler = settings.storageHandler()
      val filesToLoad = listStageFiles(domain, dryMode = false).flatMap { case (_, files) => files }
      val destFolder = DatasetArea.stage(domain.name)
      val inputDir = new Path(domain.resolveDirectory())
      Utils.println(s"Moving ${filesToLoad.size} file(s) from $inputDir to $destFolder")
      filesToLoad.foreach { file =>
        logger.info(s"Importing $file")
        val destFile = new Path(destFolder, file.path.getName)
        Utils.println(s"Moving ${file.path.getName}")
        storageHandler.moveFromLocal(file.path, destFile)
      }

    }
  }

  def listStageFiles(domain: Domain, dryMode: Boolean): Map[String, List[FileInfo]] = {
    val inputDir = new Path(domain.resolveDirectory())
    val storageHandler = settings.storageHandler()
    if (storageHandler.exists(inputDir)) {
      logger.info(s"Scanning $inputDir")

      val domainFolderFilesInfo = storageHandler
        .list(inputDir, recursive = false)
        .filterNot(
          _.path.getName.startsWith(".")
        ) // ignore files starting with '.' aka .DS_Store

      val zippedFileExtensions = List(".gz", ".tgz", ".zip")

      def getFileWithoutExt(file: FileInfo): Path = {
        val fileName = file.path.getName
        fileName.lastIndexOf('.') match {
          case -1 => file.path
          case i  => new Path(file.path.getParent, fileName.substring(0, i))
        }
      }

      val zippedFiles = domainFolderFilesInfo
        .filter(fileInfo =>
          zippedFileExtensions.contains(
            fileInfo.path.getName.split('.').lastOption.getOrElse("")
          )
        )
        .map(zippedFileInfo => {
          val zippedFileWithoutExt = getFileWithoutExt(zippedFileInfo)
          val zippedOutputDirectory =
            new Path(zippedFileWithoutExt.getParent, zippedFileWithoutExt.getName)
          storageHandler.mkdirs(zippedOutputDirectory)
          // We use Java nio to handle archive files

          // Java nio FileSystems are only registered based on the system the JVM is based on
          // To handle external FileSystems (ex: GCS from a linux OS), we need to manually install them
          // As long as the user provide this app classpath with the appropriate FileSystem implementations,
          // we can virtually handle anything
          def asBetterFile(path: Path): File = {
            Try {
              StorageHandler.localFile(path) // try once
            } match {
              case Success(file) => file
              // There is no FileSystem registered that can handle this URI
              case Failure(_: ProviderNotFoundException) =>
                FileSystems
                  .newFileSystem(
                    path.toUri,
                    Collections.emptyMap[String, Any](),
                    getClass.getClassLoader
                  )
                // Try to install one from the classpath
                File(path.toUri) // retry
              case Failure(exception) => throw exception
            }
          }

          // File is a proxy to java.nio.Path / working with Uri
          val tmpDir = asBetterFile(zippedOutputDirectory)
          val zippedFile = asBetterFile(zippedFileInfo.path)
          zippedFile.extension() match {
            case Some(".tgz") => Unpacker.unpack(zippedFile, tmpDir)
            case Some(".gz")  => zippedFile.unGzipTo(tmpDir / zippedOutputDirectory.getName)
            case Some(".zip") => zippedFile.unzipTo(tmpDir)
            case _            => logger.error(s"Unsupported archive type for $zippedFile")
          }
          if (!dryMode) {
            storageHandler.delete(zippedFileInfo.path)
          }
          zippedFileInfo -> zippedOutputDirectory
        })

      val results = {
        for (table <- domain.tables) yield {
          val anyLevelAck = table.metadata
            .flatMap(_.ack)
            .orElse(domain.metadata.flatMap(_.ack))
            .orElse(settings.appConfig.ack)
            .getOrElse("")

          val tableFiles =
            domainFolderFilesInfo.filter(file => table.pattern.matcher(file.path.getName).matches())

          if (anyLevelAck.nonEmpty) {
            // for a given table, the files included within each zipped file that respect the table pattern
            // should be added to the table files if and only if the corresponding zipped ack file exists
            val zippedTableFiles =
              zippedFiles
                .filter(zippedFile => {
                  val ackPath = getFileWithoutExt(zippedFile._1).suffix(s".$anyLevelAck")
                  logger.info(s"Checking zipped ack file $ackPath")
                  val ret = storageHandler.exists(ackPath)
                  if (ret && !dryMode) {
                    storageHandler.delete(ackPath)
                  }
                  ret
                })
                .flatMap(zippedFile => {
                  storageHandler
                    .list(zippedFile._2, recursive = false)
                    .filter(fileInfo => table.pattern.matcher(fileInfo.path.getName).matches())
                })
            table.name -> (zippedTableFiles ++ tableFiles.filter(fileInfo => {
              val ackPath = getFileWithoutExt(fileInfo).suffix(s".$anyLevelAck")
              logger.info(s"Checking ack raw file $ackPath")
              val ret = storageHandler.exists(ackPath)
              if (ret && !dryMode) {
                storageHandler.delete(ackPath)
              }
              ret
            }))
          } else {
            val tableZippedFiles = zippedFiles
              .map(_._2)
              .flatMap(zippedOutputDirectory =>
                storageHandler
                  .list(zippedOutputDirectory, recursive = false)
                  .filter(fileInfo => table.pattern.matcher(fileInfo.path.getName).matches())
              )
            table.name -> (tableZippedFiles ++ tableFiles)
          }
        }
      }

      zippedFiles.map(_._2).foreach(storageHandler.delete)

      results.toMap
    } else {
      logger.error(s"Input path : $inputDir not found, ${domain.name} Domain is ignored")
      (for (table <- domain.tables) yield {
        table.name -> List.empty
      }).toMap
    }
  }

  /*private def listExtensionsMatchesInFolder(
    domainFolderContent: List[FileInfo],
    tablesPattern: List[Pattern]
  ): List[String] = {
    val tableExtensions: List[String] = tablesPattern.flatMap { pattern =>
      domainFolderContent.flatMap { fileInfo =>
        val fileName = fileInfo.path.getName
        if (pattern.matcher(fileName).matches()) {
          val res: Option[String] = fileName.lastIndexOf('.') match {
            case -1 => None
            case i  => Some(fileName.substring(i + 1))
          }
          res
        } else {
          None
        }
      }
    }.distinct
    logger.info(s"Found extensions : $tableExtensions")
    tableExtensions.map(ext => if (ext.startsWith(".") || ext.isEmpty) ext else s".$ext")
  }*/

  /** Split files into resolved and unresolved datasets. A file is unresolved if a corresponding
    * schema is not found. Schema matching is based on the dataset filename pattern
    *
    * @param config
    *   : includes Load pending dataset of these domain only excludes : Do not load datasets of
    *   these domains if both lists are empty, all domains are included
    */
  @nowarn
  def load(config: LoadConfig): Try[SparkJobResult] = {
    val loadResults =
      if (config.test) {
        logger.info("Test mode enabled")
        val domain = schemaHandler.getDomain(config.domains.head)
        val jobResult =
          domain match {
            case Some(dom) =>
              val schema = dom.tables.find(_.name == config.tables.head)
              schema match {
                case Some(sch) =>
                  val paths = config.files.getOrElse(Nil).map(new Path(_)).sortBy(_.getName)
                  val fileInfos = paths.map(p =>
                    FileInfo(p, 0, Instant.now())
                  ) // dummy file infos for calling AdaptiveWriteStrategy
                  val updatedSchema =
                    AdaptiveWriteStrategy
                      .adaptThenGroup(sch, fileInfos)
                      .head
                      ._1 // get the schema only
                  ingest(dom, updatedSchema, paths, config.options, config.accessToken, config.test)
                case None =>
                  throw new Exception(s"Schema ${config.tables.head} not found")
              }
            case None =>
              throw new Exception(s"Domain ${config.domains.head} not found")

          }
        List(jobResult)
      } else {
        val result =
          Try {
            val includedDomains = domainsToWatch(config)
            includedDomains.flatMap { domain =>
              logger.info(s"Watch Domain: ${domain.name}")
              val (resolved, unresolved) = pending(domain.name, config.tables.toList)
              unresolved.foreach { case (_, fileInfo) =>
                val targetPath =
                  new Path(DatasetArea.unresolved(domain.name), fileInfo.path.getName)
                logger.info(s"Unresolved file : ${fileInfo.path.getName}")
                storageHandler.move(fileInfo.path, targetPath)
              }

              val filteredResolved =
                if (settings.appConfig.privacyOnly) {
                  val (withPrivacy, _) =
                    resolved.partition { case (schema, _) =>
                      schema.exists(
                        _.attributes.map(_.resolvePrivacy()).exists(!TransformInput.None.equals(_))
                      )
                    }
                  // files for schemas without any privacy attributes are moved directly to accepted area
                  /*
                  noPrivacy.foreach {
                    case (Some(schema), fileInfo) =>
                      storageHandler.move(
                        fileInfo.path,
                        new Path(
                          new Path(DatasetArea.accepted(domain.name), schema.name),
                          fileInfo.path.getName
                        )
                      )
                    case (None, _) => throw new Exception("Should never happen")
                  }
                   */
                  withPrivacy
                } else {
                  resolved
                }

              // We group files with the same schema to ingest them together in a single step.
              val groupedResolved = filteredResolved.map {
                case (Some(schema), fileInfo) => (schema, fileInfo)
                case (None, _)                => throw new Exception("Should never happen")
              } groupBy { case (schema, _) => schema } mapValues (it =>
                it.map { case (_, fileInfo) => fileInfo }
              )

              case class JobContext(
                domain: Domain,
                schema: Schema,
                paths: List[Path],
                options: Map[String, String],
                accessToken: Option[String]
              )
              groupedResolved.toList
                .flatMap { case (schema, pendingPaths) =>
                  AdaptiveWriteStrategy.adaptThenGroup(schema, pendingPaths)
                }
                .map { case (schema, pendingPaths) =>
                  logger.info(s"""Ingest resolved file : ${pendingPaths
                      .map(_.path.getName)
                      .mkString(",")} as table ${domain.name}.${schema.name}""")

                  // We group by groupedMax to avoid rateLimit exceeded when the number of grouped files is too big for some cloud storage rate limitations.
                  val groupedPendingPathsIterator =
                    pendingPaths.grouped(settings.appConfig.groupedMax)
                  groupedPendingPathsIterator.flatMap { pendingPaths =>
                    val ingestingPaths = pendingPaths.map { pendingPath =>
                      val ingestingPath =
                        new Path(DatasetArea.ingesting(domain.name), pendingPath.path.getName)
                      if (!storageHandler.move(pendingPath.path, ingestingPath)) {
                        logger.error(s"Could not move $pendingPath to $ingestingPath")
                      }
                      ingestingPath
                    }
                    val jobs = if (settings.appConfig.grouped) {
                      JobContext(
                        domain,
                        schema,
                        ingestingPaths.toList,
                        config.options,
                        config.accessToken
                      ) :: Nil
                    } else {
                      // We ingest all the files but return false if one of them fails.
                      ingestingPaths.map { path =>
                        JobContext(domain, schema, path :: Nil, config.options, config.accessToken)
                      }
                    }
                    implicit val forkJoinTaskSupport =
                      ParUtils.createForkSupport(Some(settings.appConfig.sparkScheduling.maxJobs))
                    val parJobs =
                      ParUtils.makeParallel(jobs.toList)
                    val res = parJobs.map { jobContext =>
                      ingest(
                        jobContext.domain,
                        jobContext.schema,
                        jobContext.paths,
                        jobContext.options,
                        jobContext.accessToken,
                        config.test
                      )
                    }.toList
                    forkJoinTaskSupport.foreach(_.forkJoinPool.shutdown())
                    res
                  }
                }
            }.flatten
          }
        result match {
          case Success(jobs) =>
            jobs
          case Failure(exception) =>
            logger.error("Error during ingestion", exception)
            List(Failure(exception))
        }
      }
    val (successLoads, failureLoads) = loadResults.partition(_.isSuccess)
    val exceptionsAsString = failureLoads.map { f =>
      Utils.exceptionAsString(f.failed.get)
    }

    val successAsJobResult =
      successLoads
        .flatMap {
          case Success(result: SparkJobResult) =>
            Some(result.counters.getOrElse(IngestionCounters(0, 0, 0)))
          case Success(_) => None
        }

    val counters =
      successAsJobResult.fold(IngestionCounters(0, 0, 0)) { (acc, c) =>
        IngestionCounters(
          acc.inputCount + c.inputCount,
          acc.acceptedCount + c.acceptedCount,
          acc.rejectedCount + c.rejectedCount
        )
      }
    if (exceptionsAsString.nonEmpty) {
      logger.error("Some tables failed to be loaded")
      logger.error(exceptionsAsString.mkString("\n"))
      Failure(new Exception(exceptionsAsString.mkString("\n")))
    } else {
      logger.info("All tables loaded successfully")
      Success(SparkJobResult(None, Some(counters)))
    }
  }

  private def domainsToWatch(config: LoadConfig): List[Domain] = {
    val includedDomains = config.domains match {
      case Nil =>
        domains()
      case _ =>
        domains(config.domains.toList, config.tables.toList)
    }
    logger.info(s"Domains that will be watched: ${includedDomains.map(_.name).mkString(",")}")
    includedDomains
  }

  /** @param domainName
    *   : Domaine name
    * @return
    *   resolved && unresolved schemas / path
    */
  private def pending(
    domainName: String,
    schemasName: List[String]
  ): (Iterable[(Option[Schema], FileInfo)], Iterable[(Option[Schema], FileInfo)]) = {
    val pendingArea = DatasetArea.stage(domainName)
    logger.info(s"List files in $pendingArea")
    val loadStrategy =
      Utils
        .loadInstance[LoadStrategy](settings.appConfig.loadStrategyClass)

    val files =
      loadStrategy
        .list(settings.storageHandler(), pendingArea, recursive = false)

    if (files.nonEmpty)
      logger.info(s"Found ${files.mkString(",")}")
    else
      logger.info(s"No Files Found.")
    val domain = schemaHandler.getDomain(domainName)

    val filteredFiles = (dom: Domain) =>
      if (schemasName.nonEmpty) {
        logger.info(
          s"We will only watch files that match the schemas name:" +
          s" $schemasName for the Domain: ${dom.name}"
        )
        files.filter(f => predicate(dom, schemasName, f.path))
      } else {
        logger.info(
          s"We will watch all the files for the Domain:" +
          s" ${dom.name}"
        )
        files
      }

    val schemas = for {
      dom <- domain.toList
      (schema, path) <- filteredFiles(dom).map { file =>
        (dom.findSchema(file.path.getName), file)
      }
    } yield {
      logger.info(
        s"Found Schema ${schema.map(_.name).getOrElse("None")} for file $path"
      )
      (schema, path)
    }

    if (schemas.isEmpty) {
      logger.info(s"No Files found to ingest.")
    }
    schemas.partition { case (schema, _) => schema.isDefined }
  }

  private def predicate(domain: Domain, schemasName: List[String], file: Path): Boolean = {
    schemasName.exists { schemaName =>
      val schema = domain.tables.find(_.name.equals(schemaName))
      schema.exists(_.pattern.matcher(file.getName).matches())
    }
  }

  /** Ingest the file (called by the cron manager at ingestion time for a specific dataset
    */
  def load(config: IngestConfig): Try[JobResult] = {
    if (config.domain.isEmpty || config.schema.isEmpty) {
      val domainToWatch = if (config.domain.nonEmpty) List(config.domain) else Nil
      val schemasToWatch = if (config.schema.nonEmpty) List(config.schema) else Nil
      load(
        LoadConfig(
          domains = domainToWatch,
          tables = schemasToWatch,
          options = config.options,
          accessToken = config.accessToken,
          test = false,
          files = None
        )
      )
    } else {
      val lockPath =
        new Path(settings.appConfig.lock.path, s"${config.domain}_${config.schema}.lock")
      val locker = new FileLock(lockPath, storageHandler)
      val waitTimeMillis = settings.appConfig.lock.timeout

      locker.doExclusively(waitTimeMillis) {
        val domainName = config.domain
        val schemaName = config.schema
        val ingestingPaths = config.paths
        val result =
          for {
            domain <- domains(domainName :: Nil, schemaName :: Nil).find(_.name == domainName)
            schema <- domain.tables.find(_.name == schemaName)
          } yield ingest(
            domain,
            schema,
            ingestingPaths,
            config.options,
            config.accessToken,
            test = false
          )
        result match {
          case None =>
            Success(SparkJobResult(None, None))
          case Some(Success(jobResult)) =>
            Success(jobResult)
          case Some(Failure(exception)) =>
            Failure(exception)
        }
      }
    }
  }

  @nowarn
  def ingest(
    domain: Domain,
    schema: Schema,
    ingestingPath: List[Path],
    options: Map[String, String],
    accessToken: Option[String],
    test: Boolean
  ): Try[JobResult] = {
    val metadata = schema.mergedMetadata(domain.metadata)
    Utils.println(
      s"""Loading
         |Format: ${metadata.resolveFormat()}
         |File(s): ${ingestingPath.mkString(",")}
         |Table: ${domain.finalName}.${schema.finalName}""".stripMargin
    )
    logger.debug(
      s"Ingesting domain: ${domain.name} with schema: ${schema.name}"
    )

    val ingestionResult = Try {
      val optionsAndEnvVars = schemaHandler.activeEnvVars() ++ options
      metadata.resolveFormat() match {
        case Format.PARQUET =>
          new ParquetIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars,
            accessToken,
            test
          ).run()
        case Format.GENERIC =>
          new GenericIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars,
            accessToken,
            test
          ).run()
        case Format.DSV =>
          new DsvIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars,
            accessToken,
            test
          ).run()
        case Format.JSON_FLAT | Format.JSON =>
          new JsonIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars,
            accessToken,
            test
          ).run()
        case Format.XML =>
          new XmlIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars,
            accessToken,
            test
          ).run()
        case Format.TEXT_XML =>
          new XmlSimplePrivacyJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars,
            accessToken,
            test
          ).run()
        case Format.POSITION =>
          new PositionIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars,
            accessToken,
            test
          ).run()
        case Format.KAFKA =>
          new KafkaIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars,
            FILE,
            accessToken,
            test
          ).run()
        case Format.KAFKASTREAM =>
          new KafkaIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars,
            STREAM,
            accessToken,
            test
          ).run()
        case _ =>
          throw new Exception("Should never happen")
      }
    }
    ingestionResult match {
      case Success(Success(jobResult)) =>
        if (test) {
          logger.info(s"Test mode enabled, no file will be deleted")
        } else if (settings.appConfig.archive) {
          implicit val forkJoinTaskSupport =
            ParUtils.createForkSupport(Some(settings.appConfig.maxParCopy))
          val parIngests =
            ParUtils.makeParallel(ingestingPath)
          parIngests.foreach { ingestingPath =>
            val archivePath =
              new Path(DatasetArea.archive(domain.name), ingestingPath.getName)
            logger.info(s"Backing up file $ingestingPath to $archivePath")
            val _ = storageHandler.move(ingestingPath, archivePath)
          }
          forkJoinTaskSupport.foreach(_.forkJoinPool.shutdown())
        } else {
          logger.info(s"Deleting file $ingestingPath")
          ingestingPath.foreach(storageHandler.delete)
        }
        Success(jobResult)
      case Success(Failure(exception)) =>
        Utils.logException(logger, exception)
        Failure(exception)
      case Failure(exception) =>
        Utils.logException(logger, exception)
        Failure(exception)
    }
  }

  def inferSchema(config: InferSchemaConfig): Try[Path] = {
    // domain name if not specified will be the containing folder name
    val domainName =
      if (config.domainName.isEmpty) {
        val file = File(config.inputPath)
        file.parent.name
      } else {
        config.domainName
      }

    val saveDir = config.outputDir.getOrElse(DatasetArea.load.toString)

    // table name if not specified will be the file name without extension and delta part if any product-delta.csv
    val (name, write) = config.extractTableNameAndWriteMode()
    val tableName =
      if (config.schemaName.isEmpty)
        name
      else
        config.schemaName
    val result = (new InferSchemaJob).infer(
      domainName = domainName,
      tableName = tableName,
      pattern = None,
      comment = None,
      inputPath = config.inputPath,
      saveDir = if (saveDir.isEmpty) DatasetArea.load.toString else saveDir,
      forceFormat = config.format,
      writeMode = config.write.getOrElse(write),
      rowTag = config.rowTag,
      clean = config.clean
    )(settings.storageHandler())
    Utils.logFailure(result, logger)
    result
  }

  def inferDDL(config: Yml2DDLConfig): Try[Unit] = {
    val result = new Yml2DDLJob(config, schemaHandler).run()
    Utils.logFailure(result, logger)
  }

  def buildTask(
    config: TransformConfig
  ): AutoTask = {
    val taskDesc =
      schemaHandler
        .taskOnly(config.name, reload = true)
        .getOrElse(throw new Exception(s"Invalid task name ${config.name}"))
    logger.debug(taskDesc.toString)
    val updatedTaskDesc =
      config.query match {
        case Some(_) =>
          taskDesc.copy(sql = config.query)
        case None =>
          taskDesc
      }
    AutoTask.task(
      None,
      updatedTaskDesc,
      config.options,
      config.interactive,
      config.truncate,
      config.test,
      taskDesc.getRunEngine(),
      logExecution = true,
      config.accessToken,
      resultPageSize = 1,
      dryRun = config.dryRun
    )(
      settings,
      storageHandler,
      schemaHandler
    )
  }

  def compileAutoJob(config: TransformConfig): Try[(String, String)] = Try {
    val action = buildTask(config)
    // TODO Interactive compilation should check table existence
    val sqlWhenTableDontExist = action.buildAllSQLQueries(None, Some(false))
    val sqlWhenTableExist = action.buildAllSQLQueries(None, Some(true))
    val tableExists = Try(action.tableExists)

    val (formattedDontExist, formattedExist) =
      if (config.format) {
        (
          SQLUtils.format(sqlWhenTableDontExist, JSQLFormatter.OutputFormat.PLAIN),
          SQLUtils.format(sqlWhenTableExist, JSQLFormatter.OutputFormat.PLAIN)
        )
      } else {
        (sqlWhenTableDontExist, sqlWhenTableExist)
      }

    val result =
      (
        s"""
         |--------------------------------
         |-- SQL when table does not exist
         |--------------------------------
         |$formattedDontExist
         |""".stripMargin,
        s"""
         |--------------------------------
         |-- SQL when table exists
         |--------------------------------
         |$formattedExist
         |""".stripMargin
      )
    Utils.println(result._1)
    Utils.println(result._2)
    result
  }

  def transform(
    dependencyTree: List[TaskViewDependencyNode],
    options: Map[String, String]
  ): Try[String] = {
    implicit val forkJoinTaskSupport =
      ParUtils.createForkSupport(Some(settings.appConfig.maxParTask))

    val parJobs =
      ParUtils.makeParallel(dependencyTree)
    val res = parJobs.map { jobContext =>
      logger.info(s"Transforming ${jobContext.data.name}")
      val ok = transform(jobContext.children, options)
      if (ok.isSuccess) {
        if (jobContext.isTask()) {
          val res = transform(TransformConfig(jobContext.data.name, options))
          res
        } else
          Success("")
      } else
        ok
    }
    forkJoinTaskSupport.foreach(_.forkJoinPool.shutdown())
    val allIsSuccess = res.forall(_.isSuccess)
    if (res == Nil) {
      Success("")
    } else if (allIsSuccess) {
      res.iterator.next()
    } else {
      res.iterator.find(_.isFailure).getOrElse(throw new Exception("Should never happen"))
    }
  }

  private def testsLog(transformResults: List[StarlakeTestResult]): JobResult = {
    val (success, failure) = transformResults.partition(_.success)
    println(s"Tests run: ${transformResults.size} ")
    println(s"Tests succeeded: ${success.size}")
    println(s"Tests failed: ${failure.size}")
    if (failure.nonEmpty) {
      println(
        s"Tests failed: ${failure.map { t => s"${t.domainName}.${t.taskName}.${t.testName}" }.mkString("\n")}"
      )
    }
    if (success.nonEmpty) {
      println(
        s"Tests succeeded: ${success.map { t => s"${t.domainName}.${t.taskName}.${t.testName}" }.mkString("\n")}"
      )
    }
    if (failure.nonEmpty) {
      FailedJobResult
    } else {
      EmptyJobResult
    }
  }

  def testLoad(config: StarlakeTestConfig): (List[StarlakeTestResult], StarlakeTestCoverage) = {
    val loadTests = StarlakeTestData.loadTests(
      load = true,
      config.domain.getOrElse(""),
      config.table.getOrElse(""),
      config.test.getOrElse("")
    )
    StarlakeTestData.runLoads(loadTests, config)
  }

  def testTransform(
    config: StarlakeTestConfig
  ): (List[StarlakeTestResult], StarlakeTestCoverage) = {
    val transformTests = StarlakeTestData.loadTests(
      load = false,
      config.domain.getOrElse(""),
      config.table.getOrElse(""),
      config.test.getOrElse("")
    )
    StarlakeTestData.runTransforms(transformTests, config)
  }

  def testLoadAndTransform(
    config: StarlakeTestConfig
  ): (List[StarlakeTestResult], StarlakeTestCoverage) = {
    val loadResults =
      if (config.runLoad()) {
        testLoad(config)
      } else
        (Nil, StarlakeTestCoverage(Set.empty, Set.empty, Nil, Nil))
    val transformResults =
      if (config.runTransform()) {
        testTransform(config)
      } else
        (Nil, StarlakeTestCoverage(Set.empty, Set.empty, Nil, Nil))
    if (config.generate) {
      StarlakeTestResult.html(loadResults, transformResults, config.outputDir)
      testsLog(loadResults._1 ++ transformResults._1)
    } else {
      import java.io.{File => JFile}
      val rootFolder =
        config.outputDir
          .flatMap { dir =>
            val file = new JFile(dir)
            if (file.exists() || file.mkdirs()) {
              Option(new Directory(file))
            } else {
              Console.err.println(s"Could not create output directory $dir")
              None
            }
          }
          .getOrElse(new Directory(new JFile(settings.appConfig.root, "test-reports")))
      StarlakeTestResult.junitXml(loadResults._1, transformResults._1, rootFolder)
    }
    (loadResults._1 ++ transformResults._1, loadResults._2.merge(transformResults._2))
  }

  def autoJob(config: TransformConfig): Try[String] = {
    val result =
      if (config.recursive) {
        Utils.println(s"Building dependency tree for task ${config.name}...")
        val taskConfig = AutoTaskDependenciesConfig(tasks = Some(List(config.name)))
        val dependencyTree = new AutoTaskDependencies(settings, schemaHandler, storageHandler)
          .jobsDependencyTree(taskConfig)
        Utils.println(s"Dependency tree built for task ${config.name}")
        dependencyTree.foreach(_.print())
        transform(dependencyTree, config.options)
      } else if (config.tags.nonEmpty) {
        val jobs =
          schemaHandler.jobs().flatMap { job =>
            val tasks = job.tasks.filter { task =>
              task.tags.intersect(config.tags.toSet).nonEmpty
            }
            if (tasks.isEmpty)
              None
            else
              Some(job.copy(tasks = tasks))
          }
        Try {
          jobs
            .flatMap { job =>
              job.tasks.map { task =>
                val result = transform(config.copy(name = s"${job.name}.${task.name}"))
                result match {
                  case Success(res) =>
                    res
                  case Failure(e) =>
                    throw e
                }
              }
            }
            .mkString("\n")
        }
      } else {
        transform(config)
      }
    result
  }

  /** Successively run each task of a job
    *
    * @param transformConfig
    *   : job name as defined in the YML file and sql parameters to pass to SQL statements.
    */
  // scalastyle:off println
  def transform(transformConfig: TransformConfig): Try[String] = {
    if (transformConfig.reload)
      schemaHandler.tasks(transformConfig.reload)
    val action = buildTask(transformConfig)
    logger.info(s"Transforming with config $transformConfig")
    logger.info(s"Entering ${action.taskDesc.getRunEngine()} engine")
    Utils.println(s"""Running task ${transformConfig.name} with configuration:
         |${transformConfig.toString}
         |""".stripMargin)
    val runEngine = action.taskDesc.getRunEngine()
    Utils.println(s"Using engine $runEngine")
    runEngine match {
      case BQ =>
        val result = action.run()
        Utils.logFailure(result, logger)
        result match {
          case Success(res) =>
            transformConfig.interactive match {
              case Some(format) =>
                val bqJobResult = res.asInstanceOf[BigQueryJobResult]
                val pretty = bqJobResult.prettyPrint(format, transformConfig.dryRun)
                Success(pretty)
              case None =>
                Success("")
            }
          case Failure(e) =>
            Failure(e)
        }
      case Engine.JDBC =>
        (action.run(), transformConfig.interactive) match {
          case (Success(jdbcJobResult: JdbcJobResult), Some(format)) =>
            val pretty = jdbcJobResult.prettyPrint(format)
            Success(pretty) // Sink already done in JDBC
          case (Success(_), _) =>
            Success("")
          case (Failure(exception), _) =>
            exception.printStackTrace()
            Failure(exception)
        }
      case _ =>
        (action.run(), transformConfig.interactive) match {
          case (Success(jobResult: SparkJobResult), Some(format)) =>
            val result = jobResult.prettyPrint(format)
            Success(result)
          case (Success(_), None) =>
            Success("")
          case (Failure(exception), _) =>
            exception.printStackTrace()
            Failure(exception)
          case (Success(_), _) =>
            throw new Exception("Should never happen")
        }
    }
  }

  def esLoad(config: ESLoadConfig): Try[JobResult] = {
    val res = new ESLoadJob(config, storageHandler, schemaHandler).run()
    Utils.logFailure(res, logger)
  }

  def kafkaload(config: KafkaJobConfig): Try[JobResult] = {
    val res = new KafkaJob(config, schemaHandler).run()
    Utils.logFailure(res, logger)
  }

  def jdbcload(config: JdbcConnectionLoadConfig): Try[JobResult] = {
    val loadJob = new SparkJdbcWriter(config)
    val res = loadJob.run()
    Utils.logFailure(res, logger)
  }

  /** Runs the metrics job
    *
    * @param cliConfig
    *   : Client's configuration for metrics computing
    */
  def metric(cliConfig: MetricsConfig): Try[JobResult] = {
    // Lookup for the domain given as prompt arguments, if is found then find the given schema in this domain
    val cmdArgs = for {
      domain <- schemaHandler.getDomain(cliConfig.domain)
      schema <- domain.tables.find(_.name == cliConfig.schema)
    } yield (domain, schema)

    cmdArgs match {
      case Some((domain: Domain, schema: Schema)) =>
        val result = new MetricsJob(
          None,
          domain,
          schema,
          storageHandler,
          schemaHandler
        ).run()
        Utils.logFailure(result, logger)
      case None =>
        logger.error("The domain or schema you specified doesn't exist! ")
        Failure(new Exception("The domain or schema you specified doesn't exist! "))
    }
  }
  def applyIamPolicies(accessToken: Option[String]): Try[Unit] = {
    val ignore = BigQueryLoadConfig(
      connectionRef = None,
      outputDatabase = None,
      accessToken = accessToken
    )
    schemaHandler
      .iamPolicyTags()
      .map { iamPolicyTags =>
        new BigQuerySparkJob(ignore).applyIamPolicyTags(iamPolicyTags)
      }
      .getOrElse(Success(()))
  }

  def secure(config: LoadConfig): Try[Boolean] = {
    val includedDomains = domainsToWatch(config)
    val result = includedDomains.flatMap { domain =>
      domain.tables.map { schema =>
        val metadata = schema.mergedMetadata(domain.metadata)
        val dummyIngestionJob = new DummyIngestionJob(
          domain,
          schema,
          schemaHandler.types(),
          Nil,
          storageHandler,
          schemaHandler,
          Map.empty,
          config.accessToken,
          config.test
        )
        if (settings.appConfig.isHiveCompatible()) {
          dummyIngestionJob.applyHiveTableAcl()
        } else {
          val sink = metadata.sink
            .map(_.getSink())
            .getOrElse(throw new Exception("Sink required"))

          sink match {
            case jdbcSink: JdbcSink =>
              val connectionName = jdbcSink.connectionRef
                .getOrElse(throw new Exception("JdbcSink requires a connectionRef"))
              val connection = settings.appConfig.connections(connectionName)
              dummyIngestionJob.applyJdbcAcl(connection)
            case _: BigQuerySink =>
              val database = schemaHandler.getDatabase(domain)
              val bqConfig = BigQueryLoadConfig(
                connectionRef = Some(metadata.getSinkConnectionRef()),
                outputTableId = Some(
                  BigQueryJobBase
                    .extractProjectDatasetAndTable(database, domain.name, schema.finalName)
                ),
                rls = schema.rls,
                acl = schema.acl,
                starlakeSchema = Some(schema),
                outputDatabase = database,
                accessToken = config.accessToken
              )
              val res = new BigQuerySparkJob(bqConfig).applyRLSAndCLS(forceApply = true)
              res.recover { case e =>
                Utils.logException(logger, e)
                throw e
              }

            case _ =>
              Success(true) // unknown sink, just ignore.
          }
        }
      }
    }
    if (result.exists(_.isFailure))
      Failure(new Exception("Some errors occurred during secure"))
    else
      Success(true)
  }
}
