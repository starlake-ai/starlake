package ai.starlake.job.ingest

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Cmd
import ai.starlake.job.load.LoadStrategy
import ai.starlake.schema.handlers.{FileInfo, SchemaHandler, StorageHandler}
import ai.starlake.utils.{EmptyJobResult, JobResult, PreLoadJobResult, Unpacker, Utils}
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import scopt.OParser

import java.nio.file.{FileSystems, ProviderNotFoundException}
import java.util.Collections
import scala.util.{Failure, Success, Try}

trait PreLoadCmd extends Cmd[PreLoadConfig] with StrictLogging {

  def command = "preload"

  val parser: OParser[Unit, PreLoadConfig] = {
    val builder = OParser.builder[PreLoadConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .valueName("domain1")
        .required()
        .text("Domain to pre load"),
      builder
        .opt[Seq[String]]("tables")
        .valueName("table1,table2,table3 ...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Tables to pre load"),
      builder
        .opt[String]("strategy")
        .optional()
        .action((x, c) => c.copy(strategy = PreLoadStrategy.fromString(x)))
        .text("pre load strategy"),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional(),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("Pre load arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[PreLoadConfig] =
    OParser.parse(parser, args, PreLoadConfig(domain = "", accessToken = None))

  override def run(config: PreLoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    logger.info(
      s"Pre loading domain ${config.domain} with strategy ${config.strategy.map(_.value).getOrElse("none")}"
    )

    config.strategy match {
      case Some(PreLoadStrategy.Imported) =>
        schemaHandler.domains(List(config.domain), config.tables.toList).headOption match {
          case Some(domain) =>
            val inputDir = new Path(domain.resolveDirectory())
            val storageHandler = settings.storageHandler()
            if (storageHandler.exists(inputDir)) {
              logger.info(s"Scanning $inputDir")

              val domainFolderFilesInfo = storageHandler
                .list(inputDir, "", recursive = false)
                .filterNot(
                  _.path.getName.startsWith(".")
                ) // ignore files starting with '.' aka .DS_Store

              val archivedFileExtensions = List(".gz", ".tgz", ".zip")

              def getFileWithoutExt(file: FileInfo): Path = {
                val fileName = file.path.getName
                fileName.lastIndexOf('.') match {
                  case -1 => file.path
                  case i  => new Path(file.path.getParent, fileName.substring(0, i))
                }
              }

              val archivedFiles = domainFolderFilesInfo
                .filter(fileInfo =>
                  archivedFileExtensions.contains(
                    fileInfo.path.getName.split('.').lastOption.getOrElse("")
                  )
                )
                .map(archivedFileInfo => {
                  val archivedFileWithoutExt = getFileWithoutExt(archivedFileInfo)
                  val archivedDirectory =
                    new Path(archivedFileWithoutExt.getParent, archivedFileWithoutExt.getName)
                  storageHandler.mkdirs(archivedDirectory)
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
                  val tmpDir = asBetterFile(archivedDirectory)
                  val archivedFile = asBetterFile(archivedFileInfo.path)
                  archivedFile.extension() match {
                    case Some(".tgz") => Unpacker.unpack(archivedFile, tmpDir)
                    case Some(".gz")  => archivedFile.unGzipTo(tmpDir / archivedDirectory.getName)
                    case Some(".zip") => archivedFile.unzipTo(tmpDir)
                    case _            => logger.error(s"Unsupported archive type for $archivedFile")
                  }
                  archivedFileInfo -> archivedDirectory
                })
              // for a given table, the files included within each archive that respect the table pattern
              // should be added to the count if and only if the corresponding table archived ack file exists

              val results = {
                for (table <- domain.tables) yield {
                  val tableAck = table.metadata
                    .flatMap(_.ack)
                    .orElse(domain.metadata.flatMap(_.ack))
                    .orElse(settings.appConfig.ack)
                    .getOrElse("")

                  val tableFiles = domainFolderFilesInfo.filter(file =>
                    table.pattern.matcher(file.path.getName).matches()
                  )

                  if (tableAck.nonEmpty) {
                    // for a given table, the files included within each archive that respect the table pattern
                    // should be added to the number of table files if and only if the corresponding table archived ack file exists
                    val countArchivedTableFiles =
                      archivedFiles
                        .map(_._2)
                        .filter(archivedDirectory => {
                          storageHandler.exists(
                            new Path(
                              archivedDirectory.getParent,
                              s"${archivedDirectory.getName}$tableAck"
                            )
                          )
                        })
                        .map(archivedDirectory => {
                          storageHandler
                            .list(archivedDirectory, "", recursive = false)
                            .count(fileInfo =>
                              table.pattern.matcher(fileInfo.path.getName).matches()
                            )
                        })
                        .sum
                    table.name -> (countArchivedTableFiles + tableFiles.count(fileInfo => {
                      val fileWithoutExt = getFileWithoutExt(fileInfo)
                      storageHandler.exists(
                        new Path(fileWithoutExt.getParent, s"${fileWithoutExt.getName}$tableAck")
                      )
                    }))
                  } else {
                    table.name -> (archivedFiles
                      .map(archivedFile => {
                        val archivedDirectory = archivedFile._2
                        val tableFiles =
                          storageHandler.list(archivedDirectory, "", recursive = false)
                        tableFiles
                          .count(fileInfo => table.pattern.matcher(fileInfo.path.getName).matches())
                      })
                      .sum + tableFiles.size)
                  }
                }
              }
              archivedFiles.map(_._2).foreach(storageHandler.delete)
              return Success(PreLoadJobResult(config.domain, results.toMap))
            } else {
              return Success(PreLoadJobResult(config.domain, config.tables.map(t => t -> 0).toMap))
            }
          case None =>
            return Success(PreLoadJobResult(config.domain, config.tables.map(t => t -> 0).toMap))
        }

      case Some(PreLoadStrategy.Pending) =>
        val pendingArea = DatasetArea.stage(config.domain)
        val files = settings.storageHandler().list(pendingArea, recursive = false)
        val results =
          for (table <- config.tables) yield {
            schemaHandler.getSchema(config.domain, table) match {
              case Some(schema) =>
                table -> files.count(file => schema.pattern.matcher(file.path.getName).matches())
              case None =>
                table -> 0
            }
          }

        return Success(PreLoadJobResult(config.domain, results.toMap))

      case Some(PreLoadStrategy.Ack) =>
      // TODO

      case _ =>
        return Success(PreLoadJobResult(config.domain, config.tables.map(t => t -> 1).toMap))
    }
    Success(EmptyJobResult)
  }
}

object PreLoadCmd extends PreLoadCmd
