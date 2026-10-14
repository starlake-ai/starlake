package ai.starlake.extract

import ai.starlake.schema.model.JDBCSchema
import ai.starlake.utils.YamlSerde
import com.google.cloud.bigquery._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExtractBigQuerySchemaSpec extends AnyFlatSpec with Matchers {

  private val bqSchema = Schema.of(
    Field
      .newBuilder("id", StandardSQLTypeName.INT64)
      .setMode(Field.Mode.REQUIRED)
      .setDescription("Identifier")
      .build(),
    Field
      .newBuilder("name", StandardSQLTypeName.STRING)
      .setMode(Field.Mode.NULLABLE)
      .setDescription("Name")
      .build(),
    Field
      .newBuilder("amount", StandardSQLTypeName.NUMERIC)
      .setMode(Field.Mode.NULLABLE)
      .build(),
    Field
      .newBuilder("tags", StandardSQLTypeName.STRING)
      .setMode(Field.Mode.REPEATED)
      .build()
  )

  private def tableInfo(name: String, definition: TableDefinition): TableInfo =
    TableInfo
      .newBuilder(TableId.of("my_dataset", name), definition)
      .setDescription(s"$name description")
      .build()

  private val table = tableInfo("my_table", StandardTableDefinition.of(bqSchema))
  private val view =
    tableInfo("my_view", ViewDefinition.newBuilder("SELECT 1").setSchema(bqSchema).build())
  private val materializedView = tableInfo(
    "my_materialized_view",
    MaterializedViewDefinition.newBuilder("SELECT 1").setSchema(bqSchema).build()
  )
  private val externalTable = tableInfo(
    "my_external_table",
    ExternalTableDefinition
      .newBuilder("gs://my-bucket/*.parquet", bqSchema, FormatOptions.parquet())
      .build()
  )
  private val snapshot = tableInfo(
    "my_snapshot",
    SnapshotTableDefinition
      .newBuilder()
      .setBaseTableId(TableId.of("my_dataset", "my_table"))
      .setSnapshotTime("2026-10-14T00:00:00Z")
      .setSchema(bqSchema)
      .build()
  )
  private val allObjects = List(table, view, materializedView)

  /** listTables returns the object type without the schema */
  private def listed(table: TableInfo): TableInfo =
    table.toBuilder
      .setDefinition(
        table.getDefinition[TableDefinition]().getType match {
          case TableDefinition.Type.VIEW => ViewDefinition.of("SELECT 1")
          case TableDefinition.Type.MATERIALIZED_VIEW =>
            MaterializedViewDefinition.of("SELECT 1")
          case _ => StandardTableDefinition.newBuilder().build()
        }
      )
      .build()

  private def getTable(tables: TableInfo*)(tableId: TableId): TableInfo =
    tables.find(_.getTableId == tableId).orNull

  "parseTableTypes" should "default to tables only" in {
    ExtractBigQuerySchema.parseTableTypes(Nil) shouldBe Set(TableDefinition.Type.TABLE)
  }

  it should "be case insensitive and accept spaces instead of underscores" in {
    ExtractBigQuerySchema.parseTableTypes(
      List("table", " View ", "materialized  view", "external", "Snapshot")
    ) shouldBe Set(
      TableDefinition.Type.TABLE,
      TableDefinition.Type.VIEW,
      TableDefinition.Type.MATERIALIZED_VIEW,
      TableDefinition.Type.EXTERNAL,
      TableDefinition.Type.SNAPSHOT
    )
  }

  it should "ignore table types not supported by BigQuery" in {
    ExtractBigQuerySchema.parseTableTypes(List("TABLE", "SYSTEM TABLE", "SYNONYM")) shouldBe Set(
      TableDefinition.Type.TABLE
    )
  }

  it should "fail when no table type is supported by BigQuery" in {
    an[IllegalArgumentException] should be thrownBy
    ExtractBigQuerySchema.parseTableTypes(List("SYSTEM TABLE"))
  }

  "isExtractable" should "skip views and materialized views by default" in {
    val tableTypes = ExtractBigQuerySchema.parseTableTypes(Nil)
    allObjects.filter(ExtractBigQuerySchema.isExtractable(_, tableTypes)) shouldBe List(table)
  }

  it should "keep the configured table types" in {
    val tableTypes = ExtractBigQuerySchema.parseTableTypes(List("VIEW", "MATERIALIZED_VIEW"))
    allObjects.filter(ExtractBigQuerySchema.isExtractable(_, tableTypes)) shouldBe List(
      view,
      materializedView
    )
  }

  "extractTable" should "read the same schema from tables, views and materialized views" in {
    val schemas = allObjects.flatMap(ExtractBigQuerySchema.extractTable)
    schemas.map(_.name) shouldBe List("my_table", "my_view", "my_materialized_view")
    schemas.map(_.comment) shouldBe List(
      Some("my_table description"),
      Some("my_view description"),
      Some("my_materialized_view description")
    )
    val attributes = schemas.map(_.attributes)
    attributes.distinct should have size 1
    attributes.head.map(a => (a.name, a.`type`, a.required, a.array, a.comment)) shouldBe List(
      ("id", "long", Some(true), None, Some("Identifier")),
      ("name", "string", Some(false), None, Some("Name")),
      ("amount", "decimal", Some(false), None, None),
      ("tags", "string", Some(false), Some(true), None)
    )
  }

  it should "read the schema of external tables and snapshots" in {
    List(externalTable, snapshot)
      .flatMap(ExtractBigQuerySchema.extractTable)
      .map(_.attributes) shouldBe List.fill(2)(
      ExtractBigQuerySchema.extractTable(table).map(_.attributes).getOrElse(Nil)
    )
  }

  it should "skip definitions without schema" in {
    ExtractBigQuerySchema.extractTable(
      tableInfo("no_schema", ViewDefinition.of("SELECT 1"))
    ) shouldBe None
  }

  "extractTables" should "extract tables only by default" in {
    ExtractBigQuerySchema
      .extractTables(
        allObjects.map(listed),
        ExtractBigQuerySchema.parseTableTypes(Nil),
        getTable(allObjects: _*)
      )
      .map(_.name) shouldBe List("my_table")
  }

  it should "extract tables, views and materialized views with identical schemas" in {
    val schemas = ExtractBigQuerySchema.extractTables(
      allObjects.map(listed),
      ExtractBigQuerySchema.parseTableTypes(List("TABLE", "VIEW", "MATERIALIZED_VIEW")),
      getTable(allObjects: _*)
    )
    schemas.map(_.name) shouldBe List("my_table", "my_view", "my_materialized_view")
    schemas.map(_.attributes).distinct should have size 1
  }

  it should "check the type of the fetched object when the listing does not tell it" in {
    // An EXTERNAL object listed as a standard table
    ExtractBigQuerySchema
      .extractTables(
        List(listed(externalTable)),
        ExtractBigQuerySchema.parseTableTypes(Nil),
        getTable(externalTable)
      ) shouldBe Nil
  }

  it should "skip objects that cannot be fetched or converted" in {
    val unsupportedColumn = tableInfo(
      "unsupported_column",
      StandardTableDefinition.of(
        Schema.of(Field.of("bytes", StandardSQLTypeName.BYTES))
      )
    )
    val failing = tableInfo("failing", StandardTableDefinition.of(bqSchema))
    val deleted = tableInfo("deleted", StandardTableDefinition.of(bqSchema))
    ExtractBigQuerySchema
      .extractTables(
        List(failing, deleted, unsupportedColumn, table).map(listed),
        ExtractBigQuerySchema.parseTableTypes(Nil),
        tableId =>
          if (tableId.getTable() == "failing") throw new BigQueryException(500, "failure")
          else getTable(unsupportedColumn, table)(tableId)
      )
      .map(_.name) shouldBe List("my_table")
  }

  "extract-bq-schema" should "parse the --table-types option" in {
    ExtractBigQuerySchemaCmd
      .parse(Seq("--tables", "my_dataset", "--table-types", "TABLE,VIEW"))
      .map(_.tableTypes) shouldBe Some(List("TABLE", "VIEW"))
    ExtractBigQuerySchemaCmd.parse(Seq("--tables", "my_dataset")).map(_.tableTypes) shouldBe Some(
      Nil
    )
  }

  "fromExtractSchemaConfig" should "propagate the table types and the connection" in {
    val config = ExtractBigQuerySchemaCmd.fromExtractSchemaConfig(
      ExtractSchemaConfig(connectionRef = Some("bigquery-source"), accessToken = Some("token")),
      JDBCSchema(schema = "my_dataset", tableTypes = List("TABLE", "VIEW"))
    )
    config.tableTypes shouldBe List("TABLE", "VIEW")
    config.connectionRef shouldBe Some("bigquery-source")
    config.accessToken shouldBe Some("token")
  }

  it should "keep extracting tables only when the table types are the JDBC default ones" in {
    val extractConfig = YamlSerde.deserializeYamlExtractConfig(
      """version: 1
        |extract:
        |  connectionRef: "bigquery-source"
        |  default:
        |    tableRemarks: "ignored"
        |  jdbcSchemas:
        |    - schema: "dataset_with_default_types"
        |    - schema: "dataset_with_views"
        |      tableTypes: ["TABLE", "VIEW", "MATERIALIZED VIEW"]
        |""".stripMargin,
      "extract.sl.yml"
    )
    val tableTypes = extractConfig.jdbcSchemas.getOrElse(Nil).map { jdbcSchema =>
      ExtractBigQuerySchemaCmd.fromExtractSchemaConfig(ExtractSchemaConfig(), jdbcSchema).tableTypes
    }
    tableTypes shouldBe List(Nil, List("TABLE", "VIEW", "MATERIALIZED VIEW"))
    tableTypes.map(ExtractBigQuerySchema.parseTableTypes) shouldBe List(
      Set(TableDefinition.Type.TABLE),
      Set(
        TableDefinition.Type.TABLE,
        TableDefinition.Type.VIEW,
        TableDefinition.Type.MATERIALIZED_VIEW
      )
    )
  }
}
