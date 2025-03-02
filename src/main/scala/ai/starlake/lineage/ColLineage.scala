package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.lineage.ColLineage.Table
import ai.starlake.schema.handlers.{SchemaHandler, TableWithNameOnly}
import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.JSQLColumResolver
import ai.starlake.transpiler.schema.{JdbcColumn, JdbcMetaData, JdbcResultSetMetaData}
import ai.starlake.utils.{JsonSerializer, Utils}
import better.files.File
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters._

class ColLineage(
  settings: Settings,
  schemaHandler: SchemaHandler
) extends StrictLogging {
  val domains = schemaHandler.domains()
  val tasks = schemaHandler.tasks()

  def getTableWithColumnNames(domain: String, table: String): Table = {
    domains
      .find(_.name.equalsIgnoreCase(domain))
      .flatMap { domain =>
        domain.tables.find(_.finalName.equalsIgnoreCase(table)).map { table =>
          Table(domain.name, table.name, table.attributes.map(_.getFinalName()), isTask = false)
        }
      }
      .getOrElse(Table(domain, table, Nil, isTask = false))
  }

  def extractTables(column: JdbcColumn): List[Table] = {
    val thisTable = getTableWithColumnNames(column.tableSchema, column.tableName)

    val scopeTable = getTableWithColumnNames(column.scopeSchema, column.scopeTable)
    val childTables = column.getChildren.asScala.flatMap { child =>
      extractTables(child)
    }.toList
    val alTables = thisTable :: scopeTable :: childTables
    alTables.filter(it => it.table != null && it.table.nonEmpty)
  }
  def extractTables(resultSetMetaData: JdbcResultSetMetaData): List[Table] = {
    val allTables = resultSetMetaData.getColumns.asScala
      .flatMap { column =>
        extractTables(column)
      }
      .toList
      .distinct
    val alltaskNames = schemaHandler.taskNames().map(_.toLowerCase)
    allTables.map { table =>
      val isTask = alltaskNames.contains(table.fullName.toLowerCase)
      table.copy(isTask = isTask)
    }
  }

  def colLineage(colLineageConfig: ColLineageConfig): Option[ColLineage.Lineage] = {
    val taskDesc = schemaHandler.taskOnly(colLineageConfig.task, reload = true)
    taskDesc.toOption.map { task =>
      val sqlSubst = task.sql
        .map { sql =>
          schemaHandler.substituteRefTaskMainSQL(sql, task.getRunConnection()(settings))
        }
        .getOrElse("")
      val tableNames = SQLUtils.extractTableNames(sqlSubst)
      val quoteFreeTables = tableNames.map(SQLUtils.quoteFreeTableName)
      val tablesWithColumnNames = schemaHandler.getTablesWithColumnNames(quoteFreeTables)
      colLineage(
        colLineageConfig.outputFile,
        task.domain,
        task.table,
        sqlSubst,
        tablesWithColumnNames
      )
    }
  }

  def colLineage(
    outputFile: Option[File],
    domainName: String,
    tableName: String,
    sqlSubst: String,
    tablesWithColumnNames: List[(String, TableWithNameOnly)]
  ): ColLineage.Lineage = {
    val sql = sqlSubst.replaceAll("::[a-zA-Z0-9]+", "")
    // remove all ::TEXT (type change in columns)
    var jdbcMetadata = new JdbcMetaData("", "")
    tablesWithColumnNames.foreach { case (domainName, table) =>
      val jdbcColumns = table.attrs.map { attrName => new JdbcColumn(attrName) }
      jdbcMetadata = jdbcMetadata.addTable("", domainName, table.name, jdbcColumns.asJava)
    }

    val res: JdbcResultSetMetaData =
      JSQLColumResolver
        .getResultSetMetaData(
          sql,
          JdbcMetaData.copyOf(jdbcMetadata.setErrorMode(JdbcMetaData.ErrorMode.LENIENT))
        )

    val tables = extractTables(res)
    val relations = ColLineage.extractRelations(domainName, tableName, res)
    val allTables = ColLineage.tablesInRelations(relations) ++ tables

    val finalTables = allTables
      .groupBy(t => (t.domain, t.table))
      .map { case ((domainName, tableName), table) =>
        Table(
          domainName,
          tableName,
          table.flatMap(_.columns).distinct,
          isTask = table.exists(_.isTask)
        )
      }
      .toList
    logger.whenDebugEnabled {
      relations.foreach(r => logger.debug(r.toString))
      allTables.foreach(t => logger.debug(t.toString))
      finalTables.foreach(t => logger.debug(t.toString))
    }

    val lineage = ColLineage.Lineage(finalTables, relations)
    val diagramAsStr =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(lineage)
    Utils.save(outputFile, diagramAsStr)
    lineage
  }

}

object ColLineage {
  case class Column(domain: String, table: String, column: String)
  case class Relation(from: Column, to: Column, expression: Option[String])
  case class Table(domain: String, table: String, columns: List[String], isTask: Boolean) {
    def fullName: String = s"$domain.$table"
  }
  case class Lineage(tables: List[Table], relations: List[Relation]) {
    def diff(other: Lineage) = {
      // list all tables and columns present in one of the lineages and not in the other + all the common ones
      val inThis = this.tables.map(_.table).diff(other.tables.map(_.table))
      val inOther = other.tables.map(_.table).diff(this.tables.map(_.table))
      val inThisColumns = this.tables.map { table =>
        val otherTable = other.tables.find(_.table == table.table)
        otherTable match {
          case Some(otherTable) =>
            table.columns.diff(otherTable.columns)
          case None =>
            table.columns
        }
        (table.table, table.columns)
      }
      val inOtherColumns = other.tables.map { table =>
        val thisTable = this.tables.find(_.table == table.table)
        thisTable match {
          case Some(thisTable) =>
            table.columns.diff(thisTable.columns)
          case None =>
            table.columns
        }
        (table.table, table.columns)
      }
    }
  }

  def main(args: Array[String]) = {
    val query =
      """
        |with mycte as (
        |  select o.amount, c.id, CURRENT_TIMESTAMP() as timestamp1, o.amount as amount2
        |  from `sales.orders` o, `sales.customers` c
        |  where o.customer_id = c.id
        |),
        |yourcte as (select * from mycte)
        |select id, sumx(sumy(mycte.amount + yourcte.amount) + yourcte.amount) as sum, timestamp1, amount as amount2, yourcte.amount as amount3
        |from mycte, yourcte
        |group by yourcte.id, yourcte.timestamp1
        |""".stripMargin
    val query2 =
      """
            |WITH
            |  relationships AS (
            |    SELECT rt.from_location_code,
            |           rt.to_location_code,
            |           rt.departure_datetime                                                                              AS departure_date,
            |           rt.arrival_datetime                                                                                AS arrival_date,
            |           rt.ticketno,
            |           rt.leg_number,
            |           rt.leg_direction,
            |           rt.service_direction,
            |           rt.booking_reference,
            |           fj.transaction_id,
            |           fj.ticket_id,
            |           fj.ticket_number,
            |           rt.batch_id
            |      FROM sch1.booked_travel rt
            |        LEFT JOIN (
            |        SELECT DISTINCT t.transaction_id, sdt.ticket_number, sdt.ticket_id
            |          FROM sch2.ticket_transactions t
            |            JOIN sch2.tickets sdt USING (ticket_id)
            |            JOIN sch1.reserved_travels ert ON ert.ticketno = sdt.ticket_number
            |                  ) fj ON fj.ticket_number = rt.ticketno
            |      WHERE batch_id = 234
            |  ),
            |  xaction AS (
            |    SELECT DISTINCT t.transaction_id, sdt.ticket_number, sdt.ticket_id
            |      FROM sch2.ticket_transactions t
            |        JOIN sch2.tickets sdt USING (ticket_id)
            |        JOIN relationships ert ON ert.ticketno = sdt.ticket_number
            |      WHERE ert.batch_id = 234
            |  ),
            |  from_location AS (
            |    SELECT DISTINCT l.location_id AS from_location_id, rt.from_location_code
            |      FROM sch2.locations l
            |        JOIN relationships rt ON rt.from_location_code = l.code
            |      WHERE rt.batch_id = 234
            |  ),
            |  to_location AS (
            |    SELECT DISTINCT d.location_id AS to_location_id, rt.to_location_code
            |      FROM sch2.locations d
            |        JOIN relationships rt ON rt.to_location_code = d.code
            |      WHERE rt.batch_id = 234
            |  ),
            |  departure_dates as (
            |    select distinct on (d.date_id, d.the_date) d.date_id as departure_date_id, d.the_date as departure_date
            |      from sch2.dates d
            |        join relationships r on r.departure_date = d.the_date
            |      where r.batch_id = 234
            |  ),
            |  arrival_dates as (
            |    select distinct on (d.date_id, d.the_date) d.date_id as arrival_date_id, d.the_date as arrival_date
            |      from sch2.dates d
            |        join relationships r on r.arrival_date = d.the_date
            |      where r.batch_id = 234
            |  )
            |INSERT
            |  INTO sch2.travel_legs (transaction_id, ticket_id, leg_number, leg_direction,
            |                              from_location_id, to_location_id, service_direction,
            |                              departure_date_id, departure_date, arrival_date, arrival_date_id, booking_reference)
            |SELECT r.transaction_id,
            |       r.ticket_id,
            |       r.leg_number,
            |       r.leg_direction,
            |       f.from_location_id,
            |       t.to_location_id,
            |       r.service_direction,
            |       d.departure_date_id,
            |       r.departure_date,
            |       r.arrival_date,
            |       a.arrival_date_id,
            |       r.booking_reference
            |  FROM relationships r
            |    LEFT JOIN xaction USING (transaction_id)
            |    LEFT JOIN from_location f ON f.from_location_code = r.from_location_code
            |    LEFT JOIN to_location t ON t.to_location_code = r.to_location_code
            |    left join departure_dates d on r.departure_date = d.departure_date
            |    left join arrival_dates a on a.arrival_date = r.arrival_date
            |""".stripMargin
    println(query)
    val lineage = lineageFromQuery(Array.empty, query)
    val diagramAsStr =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(lineage)
    println(diagramAsStr)
  }
  def lineageFromQuery(
    inputTables: Array[(String, Array[(String, String)])],
    query: String
  ): Lineage = {
    val tables =
      inputTables.map { case (tableName, columns) =>
        Table("", tableName, columns.map(_._1).toList, isTask = false)

      }

    val metaData: JdbcMetaData = new JdbcMetaData("", "")

    metaData.addTable(
      "sales",
      "orders",
      new JdbcColumn("customer_id"),
      new JdbcColumn("order_id"),
      new JdbcColumn("amount"),
      new JdbcColumn("seller_id")
    )
    metaData.addTable(
      "sales",
      "customers",
      new JdbcColumn("id"),
      new JdbcColumn("signup"),
      new JdbcColumn("contact"),
      new JdbcColumn("birthdate"),
      new JdbcColumn("name1"),
      new JdbcColumn("name2"),
      new JdbcColumn("id1")
    )

    inputTables
      .foreach { case (tableName, columns) =>
        val jdbcColumns = columns.map { case (attrName, attrType) => new JdbcColumn(attrName) }
        metaData.addTable("", "", tableName, jdbcColumns.toList.asJava)
      }
    val res: JdbcResultSetMetaData =
      JSQLColumResolver
        .getResultSetMetaData(query, JdbcMetaData.copyOf(metaData))
        .asInstanceOf[JdbcResultSetMetaData]

    val relations =
      extractRelations("domain", "table", res)
    val allTables = ColLineage.tablesInRelations(relations) ++ tables.toList
    val finalTables = allTables
      .groupBy(t => (t.domain, t.table))
      .map { case ((domainName, tableName), table) =>
        Table(
          domainName,
          tableName,
          table.flatMap(_.columns).distinct,
          isTask = table.exists(_.isTask)
        )
      }
      .toList
    val lineage = ColLineage.Lineage(finalTables, relations)
    lineage
  }

  private def finalColumns(column: JdbcColumn): List[JdbcColumn] = {
    val children = column.getChildren.asScala.toList
    if (children.isEmpty) {
      if (
        Option(column.columnName).isDefined && column.columnName.nonEmpty &&
        Option(column.tableName).isDefined && column.tableName.nonEmpty
      ) {
        List(column)
      } else {
        Nil
      }
    } else {
      val finalCols = children.flatMap(finalColumns)
      finalCols
    }
  }

  def extractRelations(
    domainName: String,
    tableName: String,
    columnName: String,
    expression: Option[String],
    column: JdbcColumn
  ): List[Relation] = {
    val colName = Option(columnName).getOrElse(column.columnName)
    val currentColumn = Column(domainName, tableName, colName)
    val relations =
      if (
        Option(column.tableName).isDefined &&
        column.tableName.nonEmpty &&
        Option(column.columnName).isDefined &&
        column.columnName.nonEmpty
      ) { // this is a not a function
        val parentColumn = Column(column.tableSchema, column.tableName, column.columnName)
        val immediateRelations =
          if (Option(column.scopeTable).isDefined && column.scopeTable.nonEmpty) {
            val scopeColumn = Column(column.scopeSchema, column.scopeTable, column.columnName)
            if (scopeColumn == parentColumn) {
              List(Relation(parentColumn, currentColumn, expression))
            } else {
              List(
                Relation(parentColumn, currentColumn, expression),
                Relation(scopeColumn, parentColumn, expression)
              )
            }
          } else if (expression.getOrElse("").contains("(")) {
            // This is a function, let's get all referenced table names
            val finalCols = finalColumns(column)
            val columnList =
              finalCols
                .filter(it => it.tableName != null && it.tableName.nonEmpty)
                .map { it =>
                  val col = Column(it.tableSchema, it.tableName, it.columnName)
                  Relation(col, parentColumn, expression)
                }
            columnList
          } else {
            List(Relation(parentColumn, currentColumn, expression))
          }
        val relations =
          column.getChildren.asScala.flatMap { child =>
            if (
              Option(child.tableName).isEmpty ||
              child.tableName.isEmpty ||
              Option(child.columnName).isEmpty ||
              child.columnName.isEmpty
            ) {
              Nil
            } else {
              extractRelations(
                column.tableSchema,
                column.tableName,
                column.columnName,
                Option(column.getExpression).map(_.toString),
                child
              )
            }
          }.toList

        immediateRelations ++ relations
      } else if (
        (Option(column.tableName).isEmpty || column.tableName.isEmpty) &&
        (Option(column.columnName).isDefined && column.columnName.nonEmpty)
      ) { // this is a function
        val relations =
          column.getChildren.asScala.flatMap { child =>
            extractRelations(
              domainName,
              tableName,
              colName,
              Option(column.getExpression).map(_.toString),
              child
            )
          }.toList
        relations
      } else {
        Nil
      }
    relations
  }

  def extractRelations(
    domainName: String,
    tableName: String,
    resultSetMetaData: JdbcResultSetMetaData
  ): List[Relation] = {
    val columns = resultSetMetaData.getColumns.asScala
    val labels = resultSetMetaData.getLabels.asScala
    assert(columns.size == labels.size)
    val relations =
      columns
        .zip(labels)
        .flatMap { case (column, label) =>
          val columnName = if (Option(label).isEmpty) column.columnName else label
          ColLineage.extractRelations(
            domainName,
            tableName,
            columnName,
            Option(column.getExpression).map(_.toString),
            column
          )
        }
        .toList
    relations.distinct
  }
  def tablesInRelations(relations: List[Relation]): List[Table] = {
    val tables =
      relations.flatMap { relation =>
        val table1 =
          Table(
            relation.from.domain,
            relation.from.table,
            List(relation.from.column),
            isTask = false
          )
        val table2 =
          Table(relation.to.domain, relation.to.table, List(relation.to.column), isTask = false)
        List(table1, table2)
      }
    tables
  }
}
