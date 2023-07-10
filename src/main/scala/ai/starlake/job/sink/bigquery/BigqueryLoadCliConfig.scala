package ai.starlake.job.sink.bigquery

import ai.starlake.schema.model.{AccessControlEntry, Engine, RowLevelSecurity, Schema}
import org.apache.spark.sql.DataFrame

case class BigQueryLoadCliConfig(
  connectionRef: Option[String] = None,
  source: Either[String, DataFrame] = Left(""),
  outputDatabase: Option[String] = None,
  outputDataset: Option[String] = None,
  outputTable: Option[String] = None,
  outputPartition: Option[String] = None,
  outputClustering: Seq[String] = Nil,
  sourceFormat: String = "",
  createDisposition: String = "",
  writeDisposition: String = "",
  location: Option[String] = None,
  days: Option[Int] = None,
  rls: List[RowLevelSecurity] = Nil,
  requirePartitionFilter: Boolean = false,
  engine: Engine = Engine.SPARK,
  options: Map[String, String] = Map.empty,
  partitionsToUpdate: List[String] = Nil,
  acl: List[AccessControlEntry] = Nil,
  starlakeSchema: Option[Schema] = None,
  domainTags: Set[String] = Set.empty,
  domainDescription: Option[String] = None,
  materializedView: Boolean = false
) {
  def asBigqueryLoadConfig() = BigQueryLoadConfig(
    connectionRef = connectionRef,
    source = source,
    outputTableId = Some(
      BigQueryJobBase.extractProjectDatasetAndTable(
        outputDatabase,
        outputDataset.getOrElse(throw new Exception("outputDataset must be defined")),
        outputTable.get
      )
    ),
    outputPartition = outputPartition,
    outputClustering = outputClustering,
    sourceFormat = sourceFormat,
    createDisposition = createDisposition,
    writeDisposition = writeDisposition,
    location = location,
    days = days,
    rls = rls,
    requirePartitionFilter = requirePartitionFilter,
    engine = engine,
    options = options,
    partitionsToUpdate = partitionsToUpdate,
    acl = acl,
    starlakeSchema = starlakeSchema,
    domainTags = domainTags,
    domainDescription = domainDescription,
    materializedView = materializedView,
    outputDatabase = outputDatabase
  )
}
