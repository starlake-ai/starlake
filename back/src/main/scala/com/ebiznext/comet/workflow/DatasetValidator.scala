package com.ebiznext.comet.workflow

import java.util.regex.Pattern

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.job.{DsvJob, SparkJob}
import com.ebiznext.comet.schema.handlers.{LaunchHandler, SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel.{Domain, Metadata}
import com.ebiznext.comet.schema.model.SchemaModel.Format.{DSV, JSON}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

class DatasetValidator(storageHandler: StorageHandler,
                       schemaHandler: SchemaHandler,
                       launchHandler: LaunchHandler) {

  /**
    *
    * @param domainName
    * @return resolved && unresolved schemas / path
    */
  def pending(
      domainName: String): (Iterable[(Option[SchemaModel.Schema], Path)],
                            Iterable[(Option[SchemaModel.Schema], Path)]) = {
    val paths = storageHandler.list(DatasetArea.pending(domainName))
    val domain = schemaHandler.getDomain(domainName)
    val schemas: Iterable[(Option[SchemaModel.Schema], Path)] = for {
      schema <- paths.map { path =>
        (domain.get.findSchema(path.getName), path)
      }
    } yield schema
    schemas.partition(_._1.isDefined)
  }


  def staging(domain: Domain, schema: SchemaModel.Schema, path: Path): Unit = {
    val metadata = domain.metadata.merge(schema.metadata)

    metadata.getFormat() match {
      case DSV =>
        new DsvJob(domain, schema, schemaHandler.types.types, metadata, path, storageHandler).run(null)
      case JSON=>
        throw new Exception("Not yet Implemented");
    }
  }

  def ingesting(domain: Domain, schema: SchemaModel.Schema, path: Path): Unit = {
    val targetPath = new Path(DatasetArea.ingesting(domain.name), path.getName)
    if (storageHandler.move(path, targetPath)) {
      staging(domain, schema, targetPath)
    }
  }

  def run(): Unit = {
    val domains = schemaHandler.domains
    domains.foreach { domain =>
      val (resolved, unresolved) = pending(domain.name)
        unresolved.foreach {
        case (_, path) =>
          val targetPath = new Path(DatasetArea.unresolved(domain.name), path.getName)
          storageHandler.move(path, targetPath)
      }
      resolved.foreach {
        case (Some(schema), path) => ingesting(domain, schema, path)
        case (None, _)            => throw new Exception("Should never happen")
      }
    }
  }
}
