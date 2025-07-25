package ai.starlake.migration

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.utils.JobResult
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

class ProjectMigrator(implicit settings: Settings) extends LazyLogging {

  private val metadataFolderMigrator: List[MetadataFolderMigrator] = List(
    new ExtractMetadataFolderMigrator(),
    new RefMetadataMigrator(),
    new ApplicationMetadataMigrator(),
    new LoadMetadataMigrator(),
    new TypesMetadataMigrator(),
    new DagsMetadataMigrator(),
    new EnvMetadataMigrator(),
    new TransformMetadataMigrator()
  )

  def migrate(): Try[JobResult] = {
    metadataFolderMigrator
      .foldLeft(Success(Nil): Try[List[PostMigrationAction]]) { case (acc, folderMigrator) =>
        folderMigrator.migrate() match {
          case Success(pmaList) => acc.map(_ ++ pmaList)
          case Failure(e)       => Failure(e)
        }
      }
      .flatMap { pmaList =>
        logger.info("Files migration done. Starting post migration process...")
        pmaList.foldLeft(Success(()): Try[Unit]) {
          case (Success(_), pma)     => pma.process()
          case (err @ Failure(_), _) => err
        }
      }
      .map(_ => JobResult.empty)
  }
}

sealed trait MetadataFolderMigrator extends LazyLogging {

  protected def areaPath: Path
  protected def recursive: Boolean
  protected implicit def settings: Settings
  private implicit lazy val storageHandler: StorageHandler = settings.storageHandler()
  protected def pathFilter(path: Path): Boolean = true

  /** list of migrators that can be used during yaml file migration. Order matters.
    */
  protected def fileMigrators: List[YamlFileMigrator]

  private def listYamlFiles(): List[Path] = {
    storageHandler
      .list(areaPath, extension = ".sl.yml", recursive = recursive)
      .map(_.path)
      .filter(pathFilter)
  }

  def migrate(): Try[List[PostMigrationAction]] = {
    logger.info(s"Migrating $areaPath files...")
    listYamlFiles() match {
      case Nil =>
        logger.info("No files found to migrate")
        Success(Nil)
      case yamlFiles =>
        yamlFiles
          .flatMap { configFilePath =>
            val migrators = fileMigrators.filter(_.isEligibleForMigration(configFilePath))
            if (migrators.nonEmpty) {
              Some(
                migrators.foldLeft(
                  configFilePath -> (Failure(new RuntimeException("Useless start element")): Try[
                    List[PostMigrationAction]
                  ])
                ) { (result: (Path, Try[List[PostMigrationAction]]), migrator: YamlFileMigrator) =>
                  result match {
                    case (_, Success(_)) => result
                    case (_, Failure(_)) => configFilePath -> migrator.migrate(configFilePath)
                  }
                }
              )
            } else {
              logger.warn(
                s"Could not migrate ${configFilePath.toString}, please do it manually if it makes sense."
              )
              None
            }
          }
          .foldLeft(Success(Nil): Try[List[PostMigrationAction]]) {
            case (acc, (path, Success(pma))) =>
              logger.info(s"${path.toString} has been migrated")
              acc.map(_ ++ pma)
            case (_, (path, f @ Failure(_))) =>
              logger.error(s"Could not migrate ${path.toString}, please do it manually.")
              f
          }
    }
  }
}

class ExtractMetadataFolderMigrator(implicit val settings: Settings)
    extends MetadataFolderMigrator {
  override protected val areaPath: Path = DatasetArea.extract

  override protected def recursive: Boolean = false

  /** list of migrators that can be used during yaml file migration. Order matters.
    */
  override protected def fileMigrators: List[YamlFileMigrator] =
    List(
      ExtractYamlFileMigrator,
      new DomainYamlFileMigrator(isForExtract = true),
      TableYamlFileMigrator
    )
}

class RefMetadataMigrator(implicit val settings: Settings) extends MetadataFolderMigrator {
  override protected val areaPath: Path = DatasetArea.refs()

  override protected def recursive: Boolean = false

  /** list of migrators that can be used during yaml file migration. Order matters.
    */
  override protected def fileMigrators: List[YamlFileMigrator] =
    List(RefYamlFileMigrator)
}

class ApplicationMetadataMigrator(implicit val settings: Settings) extends MetadataFolderMigrator {
  override protected val areaPath: Path =
    new Path(DatasetArea.metadata, "application.sl.yml")

  override protected def recursive: Boolean = false

  /** list of migrators that can be used during yaml file migration. Order matters.
    */
  override protected def fileMigrators: List[YamlFileMigrator] =
    List(ApplicationYamlFileMigrator)
}

class LoadMetadataMigrator(implicit val settings: Settings) extends MetadataFolderMigrator {
  override protected val areaPath: Path = DatasetArea.load

  override protected def recursive: Boolean = true

  override protected def pathFilter(path: Path): Boolean = path.getParent != areaPath

  /** list of migrators that can be used during yaml file migration. Order matters.
    */
  override protected def fileMigrators: List[YamlFileMigrator] =
    List(new DomainYamlFileMigrator(isForExtract = false), TableYamlFileMigrator)
}

class TypesMetadataMigrator(implicit val settings: Settings) extends MetadataFolderMigrator {
  override protected val areaPath: Path = DatasetArea.types

  override protected def recursive: Boolean = false

  /** list of migrators that can be used during yaml file migration. Order matters.
    */
  override protected def fileMigrators: List[YamlFileMigrator] =
    List(TypesYamlFileMigrator)
}

class DagsMetadataMigrator(implicit val settings: Settings) extends MetadataFolderMigrator {
  override protected val areaPath: Path = DatasetArea.dags

  override protected def recursive: Boolean = false

  /** list of migrators that can be used during yaml file migration. Order matters.
    */
  override protected def fileMigrators: List[YamlFileMigrator] =
    List(DagsYamlFileMigrator)
}

class EnvMetadataMigrator(implicit val settings: Settings) extends MetadataFolderMigrator {
  override protected val areaPath: Path = DatasetArea.metadata

  override protected def recursive: Boolean = false

  override protected def pathFilter(path: Path): Boolean = {
    path.getName.startsWith("env.")
  }

  /** list of migrators that can be used during yaml file migration. Order matters.
    */
  override protected def fileMigrators: List[YamlFileMigrator] =
    List(EnvYamlFileMigrator)
}

class TransformMetadataMigrator(implicit val settings: Settings) extends MetadataFolderMigrator {
  override protected val areaPath: Path = DatasetArea.transform

  override protected def recursive: Boolean = true

  override protected def pathFilter(path: Path): Boolean = path.getParent != areaPath

  /** list of migrators that can be used during yaml file migration. Order matters.
    */
  override protected def fileMigrators: List[YamlFileMigrator] =
    List(TransformYamlFileMigrator, TaskYamlFileMigrator)
}
