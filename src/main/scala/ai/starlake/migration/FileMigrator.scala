package ai.starlake.migration

import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.TypesDesc
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path

import scala.util.Try
import ai.starlake.utils.ImplicitRichPath._

trait YamlFileMigrator {

  def migrate(path: Path)(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]]

  def isEligibleForMigration(path: Path)(implicit storageHandler: StorageHandler): Boolean
}

object ExtractYamlFileMigrator extends YamlFileMigrator {
  override def migrate(
    path: Path
  )(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]] = {
    Try {
      val migratedYamlEntity = YamlSerde.deserializeYamlExtractConfig(
        storageHandler.read(path),
        path.toString,
        propageDefault = false
      )
      YamlSerde.serializeToPath(path, migratedYamlEntity)
    }.map(_ => Nil)
  }

  override def isEligibleForMigration(
    path: Path
  )(implicit storageHandler: StorageHandler): Boolean = {
    val fileContent = storageHandler.read(path)
    "(extract|jdbcSchemas|tableTypes)\\s*:".r.findFirstIn(fileContent).isDefined
  }
}

class DomainYamlFileMigrator(isForExtract: Boolean) extends YamlFileMigrator {
  override def migrate(
    path: Path
  )(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]] = {
    YamlSerde
      .deserializeYamlLoadConfig(
        storageHandler.read(path),
        path.toString,
        isForExtract = isForExtract
      )
      .map { migratedYamlEntity =>
        val (postMigrationActions, tableFileNames) = if (isForExtract) {
          val tableFileNames = migratedYamlEntity.tables
            .find(_.name == path.fileNameWithoutSlExt)
            .map { tableEntity =>
              val tableFileName = s"_table_${tableEntity.name}.sl.yml"
              YamlSerde
                .serializeToPath(
                  new Path(path.getParent, tableFileName),
                  tableEntity
                )
              tableFileName
            }
          Nil -> tableFileNames.toList
        } else {
          val tableFilesNames: List[String] = migratedYamlEntity.tables
            .map { tableEntity =>
              val tableFileName = s"${tableEntity.name}.sl.yml"
              YamlSerde
                .serializeToPath(
                  new Path(path.getParent, tableFileName),
                  tableEntity
                )
              tableFileName
            }
          val postMigrationActions =
            if (
              migratedYamlEntity.name.trim.isEmpty || path.getParent.getName == migratedYamlEntity.name
            ) {
              Nil
            } else {
              // rename domain folder at the end of migration process in order to move all tables file's as well.
              // moving during migration process will generate FileNotFoundException because of files not migrated yet but already identified to migrate.
              List(new RenameFolder(path.getParent, migratedYamlEntity.name))
            }
          postMigrationActions -> tableFilesNames
        }
        val newDomainName = if (isForExtract) path.getName else "_config.sl.yml"
        YamlSerde.serializeToPath(
          new Path(path.getParent, newDomainName),
          migratedYamlEntity.copy(tables = Nil)
        )
        if (!(tableFileNames :+ newDomainName).contains(path.getName)) {
          storageHandler.delete(path)
        }
        postMigrationActions
      }
  }

  override def isEligibleForMigration(
    path: Path
  )(implicit storageHandler: StorageHandler): Boolean = {
    (path.getName == "_config.sl.yml") || {
      val fileContent = storageHandler.read(path)
      val keywords = if (isForExtract) {
        "(load|database|tables)\\s*:"
      } else {
        "(load|database)\\s*:" // could not include tables since this may exists in domain folder. Other attributes are too ambiguous
      }
      keywords.r.findFirstIn(fileContent).isDefined
    }
  }
}

object RefYamlFileMigrator extends YamlFileMigrator {
  override def migrate(
    path: Path
  )(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]] = {
    Try {
      val migratedYamlEntity = YamlSerde.deserializeYamlRefs(
        storageHandler.read(path),
        path.toString
      )
      YamlSerde.serializeToPath(path, migratedYamlEntity)
    }.map(_ => Nil)
  }

  override def isEligibleForMigration(
    path: Path
  )(implicit storageHandler: StorageHandler): Boolean = {
    // Ref is in one file only so it is true by default
    true
  }
}

object ApplicationYamlFileMigrator extends YamlFileMigrator {
  override def migrate(
    path: Path
  )(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]] = {
    Try {
      val migratedYamlEntity = YamlSerde.deserializeYamlApplication(
        storageHandler.read(path),
        path.toString
      )
      YamlSerde.serializeToPath(path, migratedYamlEntity)
    }.map(_ => Nil)
  }

  override def isEligibleForMigration(
    path: Path
  )(implicit storageHandler: StorageHandler): Boolean = {
    // application is in one file only so it is true by default
    true
  }
}

object TableYamlFileMigrator extends YamlFileMigrator {
  override def migrate(
    path: Path
  )(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]] = {
    Try {
      val migratedYamlEntity = YamlSerde.deserializeYamlTables(
        storageHandler.read(path),
        path.toString
      )
      val tableFileNames = migratedYamlEntity.tables.map { t =>
        val tableFileName =
          if (t.name.trim.isEmpty) path.getName else s"${t.name}.sl.yml"
        YamlSerde.serializeToPath(new Path(path.getParent, tableFileName), t)
        tableFileName
      }
      if (!tableFileNames.contains(path.getName)) {
        storageHandler.delete(path)
      }
    }.map(_ => Nil)
  }

  override def isEligibleForMigration(
    path: Path
  )(implicit storageHandler: StorageHandler): Boolean = {
    val fileContent = storageHandler.read(path)
    "(attributes|table)\\s*:".r.findFirstIn(fileContent).isDefined
  }
}

object TypesYamlFileMigrator extends YamlFileMigrator {
  override def migrate(
    path: Path
  )(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]] = {
    Try {
      val migratedYamlEntity = YamlSerde.deserializeYamlTypes(
        storageHandler.read(path),
        path.toString
      )
      YamlSerde.serializeToPath(path, TypesDesc(latestSchemaVersion, migratedYamlEntity))
    }.map(_ => Nil)
  }

  override def isEligibleForMigration(
    path: Path
  )(implicit storageHandler: StorageHandler): Boolean = {
    val fileContent = storageHandler.read(path)
    "(types|primitiveType|ddlMapping)\\s*:".r.findFirstIn(fileContent).isDefined
  }
}

object DagsYamlFileMigrator extends YamlFileMigrator {
  override def migrate(
    path: Path
  )(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]] = {
    YamlSerde
      .deserializeYamlDagConfig(
        storageHandler.read(path),
        path.toString
      )
      .map { migratedYamlEntity =>
        YamlSerde.serializeToPath(path, migratedYamlEntity)
        Nil
      }
  }

  override def isEligibleForMigration(
    path: Path
  )(implicit storageHandler: StorageHandler): Boolean = {
    true
  }
}

object EnvYamlFileMigrator extends YamlFileMigrator {
  override def migrate(
    path: Path
  )(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]] = {
    Try {
      YamlSerde
        .deserializeYamlEnvConfig(
          storageHandler.read(path),
          path.toString
        )
    }
      .map { migratedYamlEntity =>
        YamlSerde.serializeToPath(path, migratedYamlEntity)
        Nil
      }
  }

  override def isEligibleForMigration(
    path: Path
  )(implicit storageHandler: StorageHandler): Boolean = {
    val filename = path.getName
    filename.startsWith("env.")
  }
}

object TransformYamlFileMigrator extends YamlFileMigrator {
  override def migrate(
    path: Path
  )(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]] = {
    YamlSerde
      .deserializeYamlTransform(
        storageHandler.read(path),
        path.toString
      )
      .map { migratedYamlEntity =>
        YamlSerde.serializeToPath(path, migratedYamlEntity)
        Nil
      }
  }

  override def isEligibleForMigration(
    path: Path
  )(implicit storageHandler: StorageHandler): Boolean = {

    path.fileNameWithoutSlExt == "_config" || {
      val fileContent = storageHandler.read(path)
      "(transform|tasks|default)\\s*:".r.findFirstIn(fileContent).isDefined
    }
  }
}

object TaskYamlFileMigrator extends YamlFileMigrator {
  override def migrate(
    path: Path
  )(implicit storageHandler: StorageHandler): Try[List[PostMigrationAction]] = {
    Try {
      YamlSerde
        .deserializeYamlTask(
          storageHandler.read(path),
          path.toString
        )
    }
      .map { migratedYamlEntity =>
        YamlSerde.serializeToPath(path, migratedYamlEntity)
        Nil
      }
  }

  override def isEligibleForMigration(
    path: Path
  )(implicit storageHandler: StorageHandler): Boolean = {
    val fileContent = storageHandler.read(path)
    "(task)\\s*:".r.findFirstIn(fileContent).isDefined
  }
}
