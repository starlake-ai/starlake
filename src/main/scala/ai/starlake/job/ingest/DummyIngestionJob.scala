package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{DomainInfo, SchemaInfo, Type}
import org.apache.hadoop.fs.Path

class DummyIngestionJob(
  val domain: DomainInfo,
  val schema: SchemaInfo,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler,
  val options: Map[String, String],
  val accessToken: Option[String],
  val test: Boolean,
  val scheduledDate: Option[String]
)(implicit val settings: Settings)
    extends IngestionJob
