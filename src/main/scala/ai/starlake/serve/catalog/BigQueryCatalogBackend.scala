package ai.starlake.serve.catalog

import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.extract.{ExtractBigQuerySchema, TablesExtractConfig}
import com.google.cloud.bigquery.BigQuery.{DatasetListOption, TableDataListOption, TableListOption}
import com.google.cloud.bigquery._

import java.time.Instant

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

  private def rowValues(row: FieldValueList, fields: List[Field]): List[Any] =
    row.asScala.toList.zipWithIndex.map { case (v, i) =>
      BigQueryCatalogBackend.jsonValue(v, fields.lift(i).map(_.getType.getStandardType))
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
        try
          bq.query(BigQueryCatalogBackend.viewPreviewJob(sql))
            .iterateAll()
            .asScala
            .take(limit + 1)
            .map(row => rowValues(row, fields))
            .toList
        catch {
          case e: Exception if BigQueryCatalogBackend.isBytesBilledExceeded(e) =>
            throw new CatalogException(
              CatalogErrorCode.QueryFailed,
              BigQueryCatalogBackend.bytesBilledMessage(schema, table),
              e
            )
        }
      } else {
        bq.listTableData(t.getTableId, TableDataListOption.pageSize(limit + 1L))
          .getValues
          .asScala
          .take(limit + 1)
          .map(row => rowValues(row, fields))
          .toList
      }
    CatalogPreview(columns, rows.take(limit), truncated = rows.size > limit)
  }
}

object BigQueryCatalogBackend {

  /** A view preview runs a real query, so its cost is capped at 1 GiB billed. */
  val MaxPreviewBytesBilled: Long = 1L << 30

  private[catalog] def viewPreviewJob(sql: String): QueryJobConfiguration =
    QueryJobConfiguration
      .newBuilder(sql)
      .setUseLegacySql(false)
      .setMaximumBytesBilled(MaxPreviewBytesBilled)
      .build()

  /** True when the failure (or one of its causes) is BigQuery refusing a query over the cap. */
  private[catalog] def isBytesBilledExceeded(e: Throwable): Boolean =
    Iterator
      .iterate(e)(_.getCause)
      .takeWhile(_ != null)
      .take(10)
      .exists {
        case b: BigQueryException =>
          b.getReason == "bytesBilledLimitExceeded" || mentionsBytesBilled(b.getMessage)
        case other => mentionsBytesBilled(other.getMessage)
      }

  private def mentionsBytesBilled(message: String): Boolean =
    message != null &&
    (message.contains("bytesBilledLimitExceeded") || message.contains("limit for bytes billed"))

  private[catalog] def bytesBilledMessage(schema: String, table: String): String =
    s"Preview of view $schema.$table would scan more than 1 GiB; query it from a worksheet instead"

  /** Converts a BigQuery cell to a JSON scalar: INT64, FLOAT64 and BOOL are typed, TIMESTAMP is
    * ISO-8601 UTC, everything else (NUMERIC, DATE, ...) stays a string. Falls back to the string
    * value when the column type is unknown or the value cannot be parsed.
    */
  private[catalog] def jsonValue(value: FieldValue, tpe: Option[StandardSQLTypeName]): Any =
    if (value == null || value.isNull) null
    else if (value.getAttribute != FieldValue.Attribute.PRIMITIVE) String.valueOf(value.getValue)
    else
      try
        tpe match {
          case Some(StandardSQLTypeName.INT64) => java.lang.Long.valueOf(value.getLongValue)
          case Some(StandardSQLTypeName.BOOL)  => java.lang.Boolean.valueOf(value.getBooleanValue)
          case Some(StandardSQLTypeName.FLOAT64) =>
            val d = value.getDoubleValue
            if (d.isNaN || d.isInfinite) d.toString else java.lang.Double.valueOf(d)
          case Some(StandardSQLTypeName.TIMESTAMP) =>
            val micros = value.getTimestampValue
            Instant
              .ofEpochSecond(
                Math.floorDiv(micros, 1000000L),
                Math.floorMod(micros, 1000000L) * 1000L
              )
              .toString
          case _ => value.getStringValue
        }
      catch {
        case NonFatal(_) => value.getStringValue
      }
}
