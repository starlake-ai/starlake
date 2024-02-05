package ai.starlake.job.sink.bigquery

import ai.starlake.schema.model._
import com.google.cloud.bigquery.TableId
import org.apache.spark.sql.DataFrame

case class BigQueryLoadConfig(
  connectionRef: Option[String],
  source: Either[String, DataFrame] = Left(""),
  outputTableId: Option[TableId] = None,
  outputPartition: Option[String] = None,
  outputClustering: Seq[String] = Nil,
  sourceFormat: String = "",
  createDisposition: String = "",
  writeDisposition: String = "",
  days: Option[Int] = None,
  rls: List[RowLevelSecurity] = Nil,
  requirePartitionFilter: Boolean = false,
  engine: Engine = Engine.SPARK,
  partitionsToUpdate: List[String] = Nil,
  acl: List[AccessControlEntry] = Nil,
  starlakeSchema: Option[Schema] = None,
  domainTags: Set[String] = Set.empty,
  domainDescription: Option[String] = None,
  materializedView: Boolean = false,
  outputTableDesc: Option[String] = None,
  attributesDesc: List[AttributeDesc] = Nil,
  outputDatabase: Option[String] = None,
  enableRefresh: Option[Boolean] = None,
  refreshIntervalMs: Option[Long] = None,
  dynamicPartitionOverwrite: Option[Boolean] = None
)
