package ai.starlake.schema.model

import com.google.cloud.bigquery.{Schema => BQSchema, TableId}

case class TableInfo(
  tableId: TableId,
  maybeTableDescription: Option[String] = None,
  maybeSchema: Option[BQSchema] = None,
  maybePartition: Option[FieldPartitionInfo] = None,
  maybeCluster: Option[ClusteringInfo] = None,
  attributesDesc: List[AttributeDesc] = Nil
)
