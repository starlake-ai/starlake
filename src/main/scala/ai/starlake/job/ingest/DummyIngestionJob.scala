package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{Domain, Schema, Type}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row}

import scala.util.Try

class DummyIngestionJob(
  val domain: Domain,
  val schema: Schema,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler,
  val options: Map[String, String],
  val accessToken: Option[String],
  val test: Boolean
)(implicit val settings: Settings)
    extends IngestionJob {
  override def loadDataSet(): Try[DataFrame] = ???

  /** ingestion algorithm
    *
    * @param dataset
    */
  override protected def ingest(dataset: DataFrame): (Dataset[String], Dataset[Row], Long) =
    throw new Exception("Should never be called. User for applying security only")

  override def defineOutputAsOriginalFormat(rejectedLines: DataFrame): DataFrameWriter[Row] = ???
}
