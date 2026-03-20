package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.extract.JdbcDbUtils.{lastExportTableName, Columns}
import ai.starlake.schema.model.*
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.LazyLogging

import java.sql.{Connection, Date, PreparedStatement, ResultSet, Timestamp}

object LastExportUtils extends LazyLogging {

  sealed trait BoundType {
    def value: Any
  }
  case class InclusiveBound(value: Any) extends BoundType
  case class ExclusiveBound(value: Any) extends BoundType
  sealed trait BoundaryDef
  case class Boundary(lower: BoundType, upper: BoundType) extends BoundaryDef
  case object Unbounded extends BoundaryDef

  sealed trait BoundariesDef

  case class Bounds(
    typ: PrimitiveType,
    count: Long,
    max: Any,
    partitions: List[Boundary]
  ) extends BoundariesDef

  case object NoBound extends BoundariesDef

  private val MIN_TS = Timestamp.valueOf("1970-01-01 00:00:00")
  private val MIN_DATE = java.sql.Date.valueOf("1970-01-01")
  private val MIN_DECIMAL = java.math.BigDecimal.ZERO

  def getBoundaries(
    conn: Connection,
    auditConnection: Connection,
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    auditColumns: Columns
  )(implicit settings: Settings): Bounds = {
    val partitionRange = 0 until tableExtractDataConfig.nbPartitions
    tableExtractDataConfig.partitionColumnType match {
      case PrimitiveType.long | PrimitiveType.short | PrimitiveType.int =>
        val lastExport =
          if (tableExtractDataConfig.fullExport)
            None
          else
            getMaxLongFromSuccessfulExport(
              auditConnection,
              extractConfig,
              tableExtractDataConfig,
              "last_long",
              auditColumns
            )
        internalBoundaries(conn, extractConfig, tableExtractDataConfig, None) { statement =>
          statement.setLong(1, lastExport.getOrElse(Long.MinValue))
          JdbcDbUtils.executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            val (min, max) = {
              tableExtractDataConfig.partitionColumnType match {
                case PrimitiveType.long => (rs.getLong(2), rs.getLong(3))
                case PrimitiveType.int  => (rs.getInt(2).toLong, rs.getInt(3).toLong)
                case _                  => (rs.getShort(2).toLong, rs.getShort(3).toLong)
              }
            }

            val interval = (max - min) / tableExtractDataConfig.nbPartitions
            val intervals = partitionRange.map { index =>
              val lower =
                if (index == 0) InclusiveBound(min) else ExclusiveBound(min + (interval * index))
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  min + (interval * (index + 1))
              Boundary(lower, InclusiveBound(upper))
            }.toList
            Bounds(
              PrimitiveType.long,
              count,
              max,
              intervals
            )
          }
        }

      case PrimitiveType.decimal =>
        val lastExport =
          if (tableExtractDataConfig.fullExport)
            None
          else
            getMaxDecimalFromSuccessfulExport(
              auditConnection,
              extractConfig,
              tableExtractDataConfig,
              "last_decimal",
              auditColumns
            )
        internalBoundaries(conn, extractConfig, tableExtractDataConfig, None) { statement =>
          statement.setBigDecimal(1, lastExport.getOrElse(MIN_DECIMAL))
          JdbcDbUtils.executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            val min = Option(rs.getBigDecimal(2)).getOrElse(MIN_DECIMAL)
            val max = Option(rs.getBigDecimal(3)).getOrElse(MIN_DECIMAL)
            val interval = max
              .subtract(min)
              .divide(new java.math.BigDecimal(tableExtractDataConfig.nbPartitions))
            val intervals = partitionRange.map { index =>
              val lower =
                if (index == 0) InclusiveBound(min)
                else ExclusiveBound(min.add(interval.multiply(new java.math.BigDecimal(index))))
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  min.add(interval.multiply(new java.math.BigDecimal(index + 1)))
              Boundary(lower, InclusiveBound(upper))
            }.toList
            Bounds(
              PrimitiveType.decimal,
              count,
              max,
              intervals
            )
          }
        }

      case PrimitiveType.date =>
        val lastExport =
          if (tableExtractDataConfig.fullExport)
            None
          else
            getMaxDateFromSuccessfulExport(
              auditConnection,
              extractConfig,
              tableExtractDataConfig,
              "last_date",
              auditColumns
            )
        internalBoundaries(conn, extractConfig, tableExtractDataConfig, None) { statement =>
          statement.setDate(1, lastExport.getOrElse(MIN_DATE))
          JdbcDbUtils.executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            val min = Option(rs.getDate(2)).getOrElse(MIN_DATE)
            val max = Option(rs.getDate(3)).getOrElse(MIN_DATE)
            val interval = (max.getTime() - min.getTime()) / tableExtractDataConfig.nbPartitions
            val intervals = partitionRange.map { index =>
              val lower =
                if (index == 0) InclusiveBound(min)
                else ExclusiveBound(new java.sql.Date(min.getTime() + (interval * index)))
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  new java.sql.Date(min.getTime() + (interval * (index + 1)))
              Boundary(lower, InclusiveBound(upper))
            }.toList
            Bounds(
              PrimitiveType.date,
              count,
              max,
              intervals
            )
          }
        }

      case PrimitiveType.timestamp =>
        val lastExport =
          if (tableExtractDataConfig.fullExport)
            None
          else
            getMaxTimestampFromSuccessfulExport(
              auditConnection,
              extractConfig,
              tableExtractDataConfig,
              "last_ts",
              auditColumns
            )
        internalBoundaries(conn, extractConfig, tableExtractDataConfig, None) { statement =>
          statement.setTimestamp(1, lastExport.getOrElse(MIN_TS))
          JdbcDbUtils.executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            val min = Option(rs.getTimestamp(2)).getOrElse(MIN_TS)
            val max = Option(rs.getTimestamp(3)).getOrElse(MIN_TS)
            val interval = (max.getTime() - min.getTime()) / tableExtractDataConfig.nbPartitions
            val intervals = partitionRange.map { index =>
              val lower =
                if (index == 0) InclusiveBound(min)
                else ExclusiveBound(new java.sql.Timestamp(min.getTime() + (interval * index)))
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  new java.sql.Timestamp(min.getTime() + (interval * (index + 1)))
              Boundary(lower, InclusiveBound(upper))
            }.toList
            Bounds(
              PrimitiveType.timestamp,
              count,
              max,
              intervals
            )
          }
        }
      case PrimitiveType.string if tableExtractDataConfig.hashFunc.isDefined =>
        if (!tableExtractDataConfig.fullExport) {
          logger.warn(
            "Delta fetching is not compatible with partition on string. Going to extract fully in parallel. To disable this warning please set fullExport in the table definition."
          )
        }
        val stringPartitionFunc = tableExtractDataConfig.hashFunc.map(
          Utils.parseJinjaTpl(
            _,
            Map(
              "col" -> s"${extractConfig.data.quoteIdentifier(tableExtractDataConfig.partitionColumn)}",
              "nb_partitions" -> tableExtractDataConfig.nbPartitions.toString
            )
          )
        )
        internalBoundaries(
          conn,
          extractConfig,
          tableExtractDataConfig,
          stringPartitionFunc
        ) { statement =>
          val (count, min, max) = statement.getParameterMetaData.getParameterType(1) match {
            case java.sql.Types.BIGINT =>
              statement.setLong(1, Long.MinValue)
              JdbcDbUtils.executeQuery(statement) { rs =>
                rs.next()
                val count = rs.getLong(1)
                // The algorithm to fetch data doesn't support null values so putting 0 as default value is OK.
                val min: Long = Option(rs.getLong(2)).getOrElse(0)
                val max: Long = Option(rs.getLong(3)).getOrElse(0)
                (count, min, max)
              }
            case java.sql.Types.INTEGER =>
              statement.setInt(1, Int.MinValue)
              JdbcDbUtils.executeQuery(statement) { rs =>
                rs.next()
                val count = rs.getLong(1)
                // The algorithm to fetch data doesn't support null values so putting 0 as default value is OK.
                val min: Long = Option(rs.getInt(2)).getOrElse(0).toLong
                val max: Long = Option(rs.getInt(3)).getOrElse(0).toLong
                (count, min, max)
              }
            case java.sql.Types.SMALLINT =>
              statement.setShort(1, Short.MinValue)
              JdbcDbUtils.executeQuery(statement) { rs =>
                rs.next()
                val count = rs.getLong(1)
                // The algorithm to fetch data doesn't support null values so putting 0 as default value is OK.
                val min: Long = Option(rs.getShort(2)).getOrElse(0.shortValue()).toLong
                val max: Long = Option(rs.getShort(3)).getOrElse(0.shortValue()).toLong
                (count, min, max)
              }
            case _ =>
              val typeName = statement.getParameterMetaData.getParameterTypeName(1)
              throw new RuntimeException(s"Type $typeName not supported for partition")
          }
          count match {
            case 0 =>
              Bounds(
                PrimitiveType.long,
                count,
                0,
                List.empty
              )
            case _ =>
              val partitions =
                (min to max).map(p => Boundary(InclusiveBound(p), ExclusiveBound(p + 1))).toList
              Bounds(
                PrimitiveType.long,
                count,
                max,
                partitions
              )
          }
        }
      case PrimitiveType.string if tableExtractDataConfig.hashFunc.isEmpty =>
        throw new Exception(
          s"Unsupported type ${tableExtractDataConfig.partitionColumnType} for column partition column ${tableExtractDataConfig.partitionColumn} in table ${tableExtractDataConfig.domain}.${tableExtractDataConfig.table}. You may define your own hash to int function via stringPartitionFunc in jdbcSchema in order to support parallel fetch. Eg: abs( hashtext({{col}}) % {{nb_partitions}} )"
        )
      case _ =>
        throw new Exception(
          s"Unsupported type ${tableExtractDataConfig.partitionColumnType} for column partition column ${tableExtractDataConfig.partitionColumn} in table ${tableExtractDataConfig.domain}.${tableExtractDataConfig.table}"
        )
    }

  }

  /** @return
    *   internal boundaries from custom sql query if given, otherwise fetch from defined table
    *   directly.
    */
  private def internalBoundaries[T](
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    hashFunc: Option[String]
  )(apply: PreparedStatement => T): T = {
    val extraCondition = tableExtractDataConfig.filterOpt.map(w => s"and $w").getOrElse("")
    val quotedColumn = extractConfig.data.quoteIdentifier(tableExtractDataConfig.partitionColumn)
    val columnToDistribute = hashFunc.getOrElse(quotedColumn)
    val dataSource = tableExtractDataConfig.sql
      .map("(" + _ + ") sl_data_source")
      .getOrElse(s"${extractConfig.data.quoteIdentifier(
          tableExtractDataConfig.domain
        )}.${extractConfig.data.quoteIdentifier(tableExtractDataConfig.table)}")
    val SQL_BOUNDARIES_VALUES =
      s"""select count($quotedColumn) as count_value, min($columnToDistribute) as min_value, max($columnToDistribute) as max_value
         |from $dataSource
         |where $columnToDistribute > ? $extraCondition""".stripMargin
    val preparedStatement = conn.prepareStatement(SQL_BOUNDARIES_VALUES)
    apply(preparedStatement)
  }

  def getMaxLongFromSuccessfulExport(
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns
  )(implicit settings: Settings): Option[Long] = getMaxValueFromSuccessfulExport(
    conn,
    extractConfig,
    tableExtractDataConfig,
    columnName,
    auditColumns,
    _.getLong(1)
  )

  def getMaxDecimalFromSuccessfulExport(
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns
  )(implicit settings: Settings): Option[java.math.BigDecimal] = getMaxValueFromSuccessfulExport(
    conn,
    extractConfig,
    tableExtractDataConfig,
    columnName,
    auditColumns,
    _.getBigDecimal(1)
  )

  def getMaxTimestampFromSuccessfulExport(
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns
  )(implicit settings: Settings): Option[Timestamp] = getMaxValueFromSuccessfulExport(
    conn,
    extractConfig,
    tableExtractDataConfig,
    columnName,
    auditColumns,
    _.getTimestamp(1)
  )

  def getMaxDateFromSuccessfulExport(
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns
  )(implicit settings: Settings): Option[Date] = getMaxValueFromSuccessfulExport(
    conn,
    extractConfig,
    tableExtractDataConfig,
    columnName,
    auditColumns,
    _.getDate(1)
  )

  private def getMaxValueFromSuccessfulExport[T](
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns,
    extractColumn: ResultSet => T
  )(implicit settings: Settings): Option[T] = {
    val auditSchema = settings.appConfig.audit.getDomain()
    def getColName(colName: String): String = extractConfig.audit.quoteIdentifier(
      getCaseInsensitiveColumnName(auditSchema, lastExportTableName, auditColumns, colName)
    )
    val normalizedColumnName = getColName(columnName)
    val domainColumn = getColName("domain")
    val schemaColumn = getColName("schema")
    val stepColumn = getColName("step")
    val successColumn = getColName("success")
    val lastExtractionSQL =
      s"""
         |select max($normalizedColumnName)
         |  from $auditSchema.$lastExportTableName
         |where
         |  $domainColumn = ?
         |  and $schemaColumn = ?
         |  and $stepColumn = ?
         |  and $successColumn""".stripMargin
    logger.debug(lastExtractionSQL)
    val preparedStatement = conn.prepareStatement(lastExtractionSQL)
    preparedStatement.setString(1, tableExtractDataConfig.domain)
    preparedStatement.setString(2, tableExtractDataConfig.table)
    preparedStatement.setString(3, "ALL")
    val rs = preparedStatement.executeQuery()
    if (rs.next()) {
      val output = extractColumn(rs)
      if (rs.wasNull()) None else Option(output)
    } else {
      None
    }
  }

  def insertNewLastExport(
    conn: Connection,
    row: DeltaRow,
    partitionColumnType: Option[PrimitiveType],
    connectionSettings: ConnectionInfo,
    auditColumns: Columns
  )(implicit settings: Settings): Int = {
    conn.setAutoCommit(true)
    val auditSchema = settings.appConfig.audit.getDomain()
    def getColName(colName: String): String = connectionSettings.quoteIdentifier(
      getCaseInsensitiveColumnName(auditSchema, lastExportTableName, auditColumns, colName)
    )
    val cols = List(
      "domain",
      "schema",
      "start_ts",
      "end_ts",
      "duration",
      "mode",
      "count",
      "success",
      "message",
      "step"
    ).map(getColName).mkString(",")
    val fullReport =
      s"""insert into $auditSchema.$lastExportTableName($cols) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    val sqlInsert =
      partitionColumnType match {
        case None | Some(PrimitiveType.string) =>
          // For string, we don't support delta. Inserting last value have no sense.
          fullReport
        case Some(partitionColumnType) =>
          val lastExportColumn = partitionColumnType match {
            case PrimitiveType.int | PrimitiveType.long | PrimitiveType.short =>
              getColName("last_long")
            case PrimitiveType.decimal   => getColName("last_decimal")
            case PrimitiveType.date      => getColName("last_date")
            case PrimitiveType.timestamp => getColName("last_ts")
            case _ =>
              throw new Exception(
                s"type $partitionColumnType not supported for partition columnToDistribute"
              )
          }
          val auditSchema = settings.appConfig.audit.getDomain()
          s"""insert into $auditSchema.$lastExportTableName($cols, $lastExportColumn) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
      }

    val preparedStatement = conn.prepareStatement(sqlInsert)
    preparedStatement.setString(1, row.domain)
    preparedStatement.setString(2, row.schema)
    preparedStatement.setTimestamp(3, row.start)
    preparedStatement.setTimestamp(4, row.end)
    preparedStatement.setInt(5, row.duration)
    preparedStatement.setString(6, WriteMode.OVERWRITE.toString)
    preparedStatement.setLong(7, row.count)
    preparedStatement.setBoolean(8, row.success)
    preparedStatement.setString(9, row.message)
    preparedStatement.setString(10, row.step)
    partitionColumnType match {
      case None =>
      case Some(partitionColumnType) =>
        partitionColumnType match {
          case PrimitiveType.int | PrimitiveType.long | PrimitiveType.short =>
            preparedStatement.setLong(11, row.lastExport.asInstanceOf[Long])
          case PrimitiveType.decimal =>
            preparedStatement.setBigDecimal(11, row.lastExport.asInstanceOf[java.math.BigDecimal])
          case PrimitiveType.date =>
            preparedStatement.setDate(11, row.lastExport.asInstanceOf[java.sql.Date])
          case PrimitiveType.timestamp =>
            preparedStatement.setTimestamp(11, row.lastExport.asInstanceOf[java.sql.Timestamp])
          case PrimitiveType.string => // do nothing. If we encounter string here, it means we have succesfully partitionned on it previously.
          case _ =>
            throw new Exception(
              s"type $partitionColumnType not supported for partition columnToDistribute"
            )
        }
    }
    preparedStatement.executeUpdate()
  }

  private def getCaseInsensitiveColumnName(
    domain: String,
    table: String,
    columns: Columns,
    columnName: String
  ): String = {
    columns
      .find(_.name.equalsIgnoreCase(columnName))
      .map(_.name)
      .getOrElse(
        throw new RuntimeException(s"Column $columnName not found in $domain.$table")
      )
  }

}

case class DeltaRow(
  domain: String,
  schema: String,
  lastExport: Any,
  start: java.sql.Timestamp,
  end: java.sql.Timestamp,
  duration: Int,
  count: Long,
  success: Boolean,
  message: String,
  step: String
)
