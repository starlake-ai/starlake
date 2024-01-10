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
import ai.starlake.job.sink.jdbc.{sparkJdbcLoader, JdbcConnectionLoadConfig}
import ai.starlake.job.sink.kafka.{KafkaJob, KafkaJobConfig}
import ai.starlake.job.transform.{AutoTask, TransformConfig}
import ai.starlake.schema.generator._
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.Engine.BQ
import ai.starlake.schema.model.Mode.{FILE, STREAM}
import ai.starlake.schema.model._
import ai.starlake.utils._
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types.{BooleanType, DataType, TimestampType}

import java.nio.file.{FileSystems, ProviderNotFoundException}
import java.util.Collections
import java.util.concurrent.ForkJoinPool
import java.util.regex.Pattern
import scala.annotation.nowarn
import scala.collection.GenSeq
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.{Failure, Success, Try}

private object StarlakeSnowflakeDialect extends JdbcDialect with SQLConfHelper {
  override def canHandle(url: String): Boolean = url.toLowerCase.startsWith("jdbc:snowflake:")
  // override def quoteIdentifier(column: String): String = column
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case TimestampType =>
      Some(JdbcType(sys.env.getOrElse("SF_TIMEZONE", "TIMESTAMP"), java.sql.Types.BOOLEAN))
    case _ => JdbcUtils.getCommonJDBCType(dt)
  }
}

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
  * @param launchHandler
  *   : Cron Manager interface
  */
class IngestionWorkflow(
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler
)(implicit settings: Settings)
    extends StrictLogging {

  import org.apache.spark.sql.jdbc.JdbcDialects

  JdbcDialects.registerDialect(StarlakeSnowflakeDialect)

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

  /** Move the files from the landing area to the pending area. files are loaded one domain at a
    * time each domain has its own directory and is specified in the "directory" key of Domain YML
    * file compressed files are uncompressed if a corresponding ack file exist. Compressed files are
    * recognized by their extension which should be one of .tgz, .zip, .gz. raw file should also
    * have a corresponding ack file before moving the files to the pending area, the ack files are
    * deleted To import files without ack specify an empty "ack" key (aka ack:"") in the domain YML
    * file. "ack" is the default ack extension searched for but you may specify a different one in
    * the domain YML file.
    */

  def loadLanding(config: ImportConfig): Try[Unit] = Try {
    val filteredDomains = config.includes match {
      case Nil => domains(Nil, Nil) // Load all domains & tables
      case _   => domains(config.includes.toList, Nil)
    }
    logger.info(
      s"Loading files from Landing Zone for domains : ${filteredDomains.map(_.name).mkString(",")}"
    )
    filteredDomains.foreach { domain =>
      val storageHandler = settings.storageHandler()
      val inputDir = new Path(domain.resolveDirectory())
      if (storageHandler.exists(inputDir)) {
        logger.info(s"Scanning $inputDir")
        val domainFolderContent = storageHandler
          .list(inputDir, "", recursive = false)
          .filterNot(_.getName().startsWith(".")) // ignore files starting with '.' aka .DS_Store

        val tablesPattern = domain.tables.map(_.pattern)
        val tableExtensions = listExtensionsMatchesInFolder(domainFolderContent, tablesPattern)

        val filesToScan =
          if (domain.getAck().nonEmpty)
            domainFolderContent.filter(_.getName.endsWith(domain.getAck()))
          else
            domainFolderContent

        filesToScan.foreach { path =>
          val (filesToLoad, tmpDir) = {
            val fileName = path.getName
            val pathWithoutLastExt = new Path(
              path.getParent,
              (domain.getAck(), fileName.lastIndexOf('.')) match {
                case ("", -1) => fileName
                case ("", i)  => fileName.substring(0, i)
                case (ack, _) => fileName.stripSuffix(ack)
              }
            )

            val zipExtensions = List(".gz", ".tgz", ".zip")
            val (existingRawFile, existingArchiveFile) = {
              val findPathWithExt = if (domain.getAck().isEmpty) {
                // the file is always the one being iterated over, we just need to check the extension
                (extensions: List[String]) => extensions.find(fileName.endsWith).map(_ => path)
              } else {
                // for each extension, look if there exists a file near the .ack one
                (extensions: List[String]) =>
                  extensions.map(pathWithoutLastExt.suffix).find(storageHandler.exists)
              }
              (
                findPathWithExt(
                  tableExtensions
                ),
                findPathWithExt(zipExtensions)
              )
            }

            if (domain.getAck().nonEmpty)
              storageHandler.delete(path)

            (existingArchiveFile, existingRawFile, storageHandler.getScheme()) match {
              case (Some(zipPath), _, "file") =>
                logger.info(s"Found compressed file $zipPath")
                storageHandler.mkdirs(pathWithoutLastExt)

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
                val tmpDir = asBetterFile(pathWithoutLastExt)
                val zipFile = asBetterFile(zipPath)
                zipFile.extension() match {
                  case Some(".tgz") => Unpacker.unpack(zipFile, tmpDir)
                  case Some(".gz")  => zipFile.unGzipTo(tmpDir / pathWithoutLastExt.getName)
                  case Some(".zip") => zipFile.unzipTo(tmpDir)
                  case _            => logger.error(s"Unsupported archive type for $zipFile")
                }
                storageHandler.delete(zipPath)
                val inZipExtensions =
                  listExtensionsMatchesInFolder(
                    storageHandler
                      .list(pathWithoutLastExt, recursive = false),
                    tablesPattern
                  )
                (
                  storageHandler
                    .list(pathWithoutLastExt, recursive = false)
                    .filter(path =>
                      inZipExtensions
                        .exists(path.getName.endsWith)
                    ),
                  Some(pathWithoutLastExt)
                )
              case (_, Some(filePath), _) =>
                logger.info(s"Found raw file $filePath")
                (List(filePath), None)
              case (_, _, _) =>
                logger.warn(s"Ignoring file for path $path")
                (List.empty, None)
            }
          }

          val destFolder = DatasetArea.pending(domain.name)
          filesToLoad.foreach { file =>
            logger.info(s"Importing $file")
            val destFile = new Path(destFolder, file.getName)
            storageHandler.moveFromLocal(file, destFile)
          }

          tmpDir.foreach(storageHandler.delete)
        }
      } else {
        logger.error(s"Input path : $inputDir not found, ${domain.name} Domain is ignored")
      }
    }
  }

  private def listExtensionsMatchesInFolder(
    domainFolderContent: List[Path],
    tablesPattern: List[Pattern]
  ): List[String] = {
    val tableExtensions: List[String] = tablesPattern.flatMap { pattern =>
      domainFolderContent.flatMap { path =>
        val fileName = path.getName
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
  }

  /** Split files into resolved and unresolved datasets. A file is unresolved if a corresponding
    * schema is not found. Schema matching is based on the dataset filename pattern
    *
    * @param config
    *   : includes Load pending dataset of these domain only excludes : Do not load datasets of
    *   these domains if both lists are empty, all domains are included
    */
  @nowarn
  def loadPending(config: LoadConfig = LoadConfig()): Try[Boolean] = Try {
    val includedDomains = domainsToWatch(config)

    val result: List[Boolean] = includedDomains.flatMap { domain =>
      logger.info(s"Watch Domain: ${domain.name}")
      val (resolved, unresolved) = pending(domain.name, config.tables.toList)
      unresolved.foreach { case (_, path) =>
        val targetPath =
          new Path(DatasetArea.unresolved(domain.name), path.getName)
        logger.info(s"Unresolved file : ${path.getName}")
        storageHandler.move(path, targetPath)
      }

      val filteredResolved = if (settings.appConfig.privacyOnly) {
        val (withPrivacy, noPrivacy) =
          resolved.partition { case (schema, _) =>
            schema.exists(_.attributes.map(_.getPrivacy()).exists(!PrivacyLevel.None.equals(_)))
          }
        // files for schemas without any privacy attributes are moved directly to accepted area
        noPrivacy.foreach {
          case (Some(schema), path) =>
            storageHandler.move(
              path,
              new Path(new Path(DatasetArea.accepted(domain.name), schema.name), path.getName)
            )
          case (None, _) => throw new Exception("Should never happen")
        }
        withPrivacy
      } else {
        resolved
      }

      // We group files with the same schema to ingest them together in a single step.
      val groupedResolved: Map[Schema, Iterable[Path]] = filteredResolved.map {
        case (Some(schema), path) => (schema, path)
        case (None, _)            => throw new Exception("Should never happen")
      } groupBy { case (schema, _) => schema } mapValues (it => it.map { case (_, path) => path })

      case class JobContext(
        domain: Domain,
        schema: Schema,
        paths: List[Path],
        options: Map[String, String]
      )
      groupedResolved.map { case (schema, pendingPaths) =>
        logger.info(s"""Ingest resolved file : ${pendingPaths
            .map(_.getName)
            .mkString(",")} with schema ${schema.name}""")

        // We group by groupedMax to avoid rateLimit exceeded when the number of grouped files is too big for some cloud storage rate limitations.
        val groupedPendingPathsIterator =
          pendingPaths.grouped(settings.appConfig.groupedMax)
        groupedPendingPathsIterator.map { pendingPaths =>
          val ingestingPaths = pendingPaths.map { pendingPath =>
            val ingestingPath = new Path(DatasetArea.ingesting(domain.name), pendingPath.getName)
            if (!storageHandler.move(pendingPath, ingestingPath)) {
              logger.error(s"Could not move $pendingPath to $ingestingPath")
            }
            ingestingPath
          }
          val jobs = if (settings.appConfig.grouped) {
            JobContext(domain, schema, ingestingPaths.toList, config.options) :: Nil
          } else {
            // We ingest all the files but return false if one of them fails.
            ingestingPaths.map { path =>
              JobContext(domain, schema, path :: Nil, config.options)
            }
          }
          val (parJobs, forkJoinPool) =
            makeParallel(jobs.toList, settings.appConfig.sparkScheduling.maxJobs)
          val res = parJobs.map { jobContext =>
            ingest(
              jobContext.domain,
              jobContext.schema,
              jobContext.paths,
              jobContext.options
            ) match {
              case Failure(e) =>
                e.printStackTrace()
                false
              case Success(r) => true
            }
          }.toList
          forkJoinPool.foreach(_.shutdown())
          res.forall(_ == true)
        }
      }
    }.flatten
    result.forall(_ == true)
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
  ): (Iterable[(Option[Schema], Path)], Iterable[(Option[Schema], Path)]) = {
    val pendingArea = DatasetArea.pending(domainName)
    logger.info(s"List files in $pendingArea")
    val files = Utils
      .loadInstance[LoadStrategy](settings.appConfig.loadStrategyClass)
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
        files.filter(f => predicate(dom, schemasName, f))
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
        (dom.findSchema(file.getName), file)
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
  def load(config: IngestConfig): Try[Boolean] = {
    if (config.domain.isEmpty || config.schema.isEmpty) {
      val domainToWatch = if (config.domain.nonEmpty) List(config.domain) else Nil
      val schemasToWatch = if (config.schema.nonEmpty) List(config.schema) else Nil
      loadPending(
        LoadConfig(
          domains = domainToWatch,
          tables = schemasToWatch,
          options = config.options
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
        val result = for {
          domain <- domains(domainName :: Nil, schemaName :: Nil).find(_.name == domainName)
          schema <- domain.tables.find(_.name == schemaName)
        } yield ingest(domain, schema, ingestingPaths, config.options)
        result match {
          case None | Some(Success(_)) => Success(true)
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
    options: Map[String, String]
  ): Try[JobResult] = {
    logger.info(
      s"Start Ingestion on domain: ${domain.name} with schema: ${schema.name} on file(s): $ingestingPath"
    )
    val metadata = schema.mergedMetadata(domain.metadata)
    logger.info(
      s"Ingesting domain: ${domain.name} with schema: ${schema.name} on file: $ingestingPath with metadata $metadata"
    )

    val ingestionResult = Try {
      val optionsAndEnvVars = schemaHandler.activeEnvVars() ++ options
      metadata.getFormat() match {
        case Format.PARQUET =>
          new ParquetIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars
          ).run()
        case Format.GENERIC =>
          new GenericIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars
          ).run()
        case Format.DSV =>
          new DsvIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars
          ).run()
        case Format.SIMPLE_JSON =>
          new SimpleJsonIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars
          ).run()
        case Format.JSON =>
          new JsonIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars
          ).run()
        case Format.XML =>
          new XmlIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars
          ).run()
        case Format.TEXT_XML =>
          new XmlSimplePrivacyJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars
          ).run()
        case Format.POSITION =>
          new PositionIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            ingestingPath,
            storageHandler,
            schemaHandler,
            optionsAndEnvVars
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
            FILE
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
            STREAM
          ).run()
        case _ =>
          throw new Exception("Should never happen")
      }
    }
    ingestionResult match {
      case Success(Success(jobResult)) =>
        if (settings.appConfig.archive) {
          val (parIngests, forkJoinPool) =
            makeParallel(ingestingPath, settings.appConfig.maxParCopy)
          parIngests.foreach { ingestingPath =>
            val archivePath =
              new Path(DatasetArea.archive(domain.name), ingestingPath.getName)
            logger.info(s"Backing up file $ingestingPath to $archivePath")
            val _ = storageHandler.move(ingestingPath, archivePath)
          }
          forkJoinPool.foreach(_.shutdown())
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

  @nowarn
  private def makeParallel[T](
    collection: List[T],
    maxPar: Int
  ): (GenSeq[T], Option[ForkJoinPool]) = {
    maxPar match {
      case 1 => (collection, None)
      case _ =>
        val parCollection = collection.par
        val forkJoinPool =
          new scala.concurrent.forkjoin.ForkJoinPool(maxPar)
        parCollection.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
        (parCollection, Some(forkJoinPool))
    }
  }

  def inferSchema(config: InferSchemaConfig): Try[File] = {
    val saveDir = config.outputDir.getOrElse(DatasetArea.load.toString)

    val result = (new InferSchemaJob).infer(
      domainName = config.domainName,
      schemaName = config.schemaName,
      pattern = None,
      comment = None,
      dataPath = config.inputPath,
      saveDir = if (saveDir.isEmpty) DatasetArea.load.toString else saveDir,
      withHeader = config.withHeader,
      forceFormat = config.format,
      writeMode = config.write.getOrElse(WriteMode.OVERWRITE)
    )
    Utils.logFailure(result, logger)
    result
  }

  def inferDDL(config: Yml2DDLConfig): Try[Unit] = {
    val result = new Yml2DDLJob(config, schemaHandler).run()
    Utils.logFailure(result, logger)
  }

  private def buildTask(
    config: TransformConfig
  ): AutoTask = {
    val taskDesc =
      schemaHandler
        .task(config.name)
        .getOrElse(throw new Exception(s"Invalid task name ${config.name}"))
    logger.info(taskDesc.toString)
    AutoTask.task(
      taskDesc,
      config.options,
      config.interactive,
      config.truncate,
      resultPageSize = 1000
    )(
      settings,
      storageHandler,
      schemaHandler
    )
  }

  def compileAutoJob(config: TransformConfig): Try[Unit] = Try {
    val action = buildTask(config)
    val engine = action.taskDesc.getEngine()
    // TODO Interactive compilation should check table existence
    val (_, mainSQL, _, _) = action.buildAllSQLQueries(tableExists = false, None, None, engine)
    val output = settings.appConfig.rootServe.map(rootServe => File(File(rootServe), "compile.log"))
    output.foreach(_.overwrite(s"""START COMPILE SQL $mainSQL END COMPILE SQL"""))
    logger.info(s"""START COMPILE SQL $mainSQL END COMPILE SQL""")
  }

  def transform(
    dependencyTree: List[TaskViewDependencyNode],
    options: Map[String, String]
  ): Boolean = {
    val (parJobs, forkJoinPool) =
      makeParallel(dependencyTree, settings.appConfig.maxParTask)
    val res = parJobs.map { jobContext =>
      val ok = transform(jobContext.children, options)
      if (ok) {
        if (jobContext.isTask()) {
          transform(TransformConfig(jobContext.data.name, options))
        } else
          true
      } else
        false
    }
    forkJoinPool.foreach(_.shutdown())
    res.forall(_ == true)
  }

  def autoJob(config: TransformConfig): Try[Boolean] = Try {
    if (config.recursive) {
      val taskConfig = AutoTaskDependenciesConfig(tasks = Some(List(config.name)))
      val dependencyTree = new AutoTaskDependencies(settings, schemaHandler, storageHandler)
        .jobsDependencyTree(taskConfig)
      dependencyTree.foreach(_.print())
      transform(dependencyTree, config.options)
    } else {
      transform(config)
    }
  }

  /** Successively run each task of a job
    *
    * @param transformConfig
    *   : job name as defined in the YML file and sql parameters to pass to SQL statements.
    */
  // scalastyle:off println
  def transform(transformConfig: TransformConfig): Boolean = {
    schemaHandler.tasks(transformConfig.reload)
    val result: Boolean = {
      val action = buildTask(transformConfig)
      logger.info(s"Transforming with config $transformConfig")
      logger.info(s"Entering ${action.taskDesc.getEngine()} engine")
      action.taskDesc.getEngine() match {
        case BQ =>
          val result = action.run()
          transformConfig.interactive match {
            case Some(format) =>
              result.map { result =>
                val bqJobResult = result.asInstanceOf[BigQueryJobResult]
                logger.info("START INTERACTIVE SQL")
                bqJobResult.show(format, settings.appConfig.rootServe)
                logger.info("END INTERACTIVE SQL")
              }
            case None =>
          }
          Utils.logFailure(result, logger)
          result match {
            case Success(res) =>
            case Failure(e) =>
              val output =
                settings.appConfig.rootServe.map(rootServe =>
                  File(File(rootServe), "transform.log")
                )
              output.foreach(_.overwrite(Utils.exceptionAsString(e)))
          }
          result.isSuccess
        case Engine.JDBC =>
          (action.run(), transformConfig.interactive) match {
            case (Success(jdbcJobResult: JdbcJobResult), Some(format)) =>
              logger.info("""START INTERACTIVE SQL""")
              jdbcJobResult.show(format, settings.appConfig.rootServe)
              logger.info("""END INTERACTIVE SQL""")
              true // Sink already done in JDBC
            case (Success(_), _) =>
              true
            case (Failure(exception), _) =>
              val output =
                settings.appConfig.rootServe.map(rootServe =>
                  File(File(rootServe), "transform.log")
                )
              output.foreach(_.overwrite(Utils.exceptionAsString(exception)))
              exception.printStackTrace()
              false
          }
        case custom =>
          logger.info(s"Entering $custom engine")

          (action.run(), transformConfig.interactive) match {
            case (Success(SparkJobResult(Some(dataFrame), _)), Some(_)) =>
              // For interactive display. Used by the VSCode plugin
              logger.info("""START INTERACTIVE SQL""")
              dataFrame.show(false)
              logger.info("""END INTERACTIVE SQL""")
              true
            case (Success(_), None) =>
              true
            case (Failure(exception), _) =>
              val output =
                settings.appConfig.rootServe.map(rootServe =>
                  File(File(rootServe), "transform.log")
                )
              output.foreach(_.overwrite(Utils.exceptionAsString(exception)))
              exception.printStackTrace()
              false
            case (Success(_), _) =>
              throw new Exception("Should never happen")
          }
      }
    }
    result
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
    val loadJob = new sparkJdbcLoader(config)
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
  def applyIamPolicies(): Try[Unit] = {
    val ignore = BigQueryLoadConfig(
      connectionRef = None,
      outputDatabase = None
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
          Map.empty
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
              val config = BigQueryLoadConfig(
                connectionRef = Some(metadata.getConnectionRef()),
                outputTableId = Some(
                  BigQueryJobBase
                    .extractProjectDatasetAndTable(database, domain.name, schema.finalName)
                ),
                sourceFormat = settings.appConfig.defaultWriteFormat,
                rls = schema.rls,
                acl = schema.acl,
                starlakeSchema = Some(schema),
                outputDatabase = database
              )
              val res = new BigQuerySparkJob(config).applyRLSAndCLS(forceApply = true)
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
