package ai.starlake.serve.catalog

import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.extract.{ExtractBigQuerySchema, TablesExtractConfig}
import com.google.cloud.bigquery.BigQuery.{DatasetListOption, TableDataListOption, TableListOption}
import com.google.cloud.bigquery._

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

class BigQueryCatalogBackend(connectionName: String, connectionInfo: ConnectionInfo)(implicit
  settings: Settings
) extends CatalogBackend {

  private def scrubbed(e: Throwable): String =
    CatalogScrubber.scrub(CatalogJson.messageOf(e), connectionInfo.options)

  private lazy val client: BigQuery =
    try
      new ExtractBigQuerySchema(TablesExtractConfig(connectionRef = Some(connectionName))).bigquery
    catch {
      case NonFatal(e) => throw new CatalogException(CatalogErrorCode.ConnectionFailed, scrubbed(e))
    }

  private def run[T](f: BigQuery => T): T = {
    val bq = client
    try f(bq)
    catch {
      case e: CatalogException => throw e
      case NonFatal(e) => throw new CatalogException(CatalogErrorCode.QueryFailed, scrubbed(e))
    }
  }

  override def schemas(): List[CatalogSchema] = run { bq =>
    bq.listDatasets(DatasetListOption.pageSize(10000))
      .iterateAll()
      .asScala
      .map(_.getDatasetId.getDataset)
      .toList
      .sorted
      .map(CatalogSchema.apply)
  }

  private def isView(t: TableDefinition.Type): Boolean =
    t == TableDefinition.Type.VIEW || t == TableDefinition.Type.MATERIALIZED_VIEW

  override def tables(schema: String): List[CatalogTable] = run { bq =>
    bq.listTables(DatasetId.of(schema), TableListOption.pageSize(10000))
      .iterateAll()
      .asScala
      .filterNot(_.getTableId.getTable.startsWith("zztmp_"))
      .map { t =>
        val definitionType = Option(t.getDefinition[TableDefinition]).map(_.getType).orNull
        CatalogTable(
          t.getTableId.getTable,
          if (isView(definitionType)) "VIEW" else "TABLE",
          Option(t.getDescription).filter(_.nonEmpty)
        )
      }
      .toList
      .sortBy(_.name)
  }

  private def getTable(bq: BigQuery, schema: String, table: String): Table =
    Option(bq.getTable(TableId.of(schema, table))).getOrElse(
      throw new CatalogException(CatalogErrorCode.QueryFailed, s"Table $schema.$table not found")
    )

  override def columns(schema: String, table: String): List[CatalogColumn] = run { bq =>
    val fields = Option(getTable(bq, schema, table).getDefinition[TableDefinition].getSchema)
      .map(_.getFields.asScala.toList)
      .getOrElse(Nil)
    fields.map { f =>
      CatalogColumn(
        name = f.getName,
        `type` = f.getType.getStandardType.name(),
        nullable = f.getMode != Field.Mode.REQUIRED,
        precision = Option(f.getPrecision).map(_.intValue()),
        scale = Option(f.getScale).map(_.intValue()),
        remarks = Option(f.getDescription).filter(_.nonEmpty)
      )
    }
  }

  private def jsonValue(value: FieldValue): Any =
    if (value == null || value.isNull) null
    else
      value.getAttribute match {
        case FieldValue.Attribute.PRIMITIVE => value.getStringValue
        case _                              => String.valueOf(value.getValue)
      }

  override def preview(schema: String, table: String, limit: Int): CatalogPreview = run { bq =>
    val t = getTable(bq, schema, table)
    val definition = t.getDefinition[TableDefinition]
    val fields = Option(definition.getSchema).map(_.getFields.asScala.toList).getOrElse(Nil)
    val columns =
      fields.map(f => CatalogPreviewColumn(f.getName, f.getType.getStandardType.name()))
    // tabledata.list is free and works on tables; views need a query
    val rows: List[List[Any]] =
      if (isView(definition.getType)) {
        val sql =
          s"SELECT * FROM `${schema.replace("`", "")}`.`${table.replace("`", "")}` LIMIT ${limit + 1}"
        bq.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())
          .iterateAll()
          .asScala
          .take(limit + 1)
          .map(row => row.asScala.toList.map(jsonValue))
          .toList
      } else {
        bq.listTableData(t.getTableId, TableDataListOption.pageSize(limit + 1L))
          .getValues
          .asScala
          .take(limit + 1)
          .map(row => row.asScala.toList.map(jsonValue))
          .toList
      }
    CatalogPreview(columns, rows.take(limit), truncated = rows.size > limit)
  }
}
