package ai.starlake.integration.extract

import ai.starlake.config.DatasetArea
import ai.starlake.extract.{ExtractBigQuerySchema, TablesExtractConfig}
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{Format, Metadata}
import better.files.File
import com.google.cloud.bigquery._

import java.util.regex.Pattern
import scala.jdk.CollectionConverters._

/** A BigQuery table extracted with extract-bq-schema, then loaded by the native loader from its
  * data, gives a table of the same schema and values: DATETIME, TIME, BYTES, BIGNUMERIC, GEOGRAPHY,
  * INTERVAL, RANGE and JSON columns included.
  */
class ExtractLoadBQTypesRoundTripIntegrationSpec extends IntegrationTestBase {
  private val sourceDataset = "starlake_it_bq_roundtrip_source"
  private val targetDataset = "starlake_it_bq_roundtrip"
  private val table = "all_types"
  private val view = "all_types_view"
  // BigQuery loads RANGE values from CSV, not from JSON
  private val ranges = "ranges"

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-roundtrip-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-roundtrip-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    // The load domain is written by the test: git keeps no empty directory
    (dir / "metadata" / "load").createDirectories()
    dir
  }

  override def theSampleFolder: File = project

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private def run(sql: String): TableResult =
    bigquery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())

  private def fields(dataset: String, name: String): List[Field] =
    bigquery
      .getTable(TableId.of(dataset, name))
      .getDefinition[TableDefinition]()
      .getSchema
      .getFields
      .asScala
      .toList

  /** @return
    *   the type, mode and parameters of each column, at every level. NUMERIC is the only type of
    *   precision 38 and scale 9: the parameters of the NUMERIC columns that the loader creates are
    *   those of an unparameterized NUMERIC.
    */
  private def describe(fields: List[Field], prefix: String = ""): List[String] =
    fields.flatMap { field =>
      val path = prefix + field.getName
      val standardType = field.getType.getStandardType
      val parameters =
        if (standardType == StandardSQLTypeName.NUMERIC) ""
        else
          List(
            Option(field.getPrecision),
            Option(field.getScale),
            Option(field.getMaxLength),
            Option(field.getRangeElementType).map(_.getType)
          ).flatten.mkString("(", ", ", ")")
      s"$path $standardType$parameters ${Option(field.getMode).getOrElse(Field.Mode.NULLABLE)}" ::
      describe(Option(field.getSubFields).map(_.asScala.toList).getOrElse(Nil), path + ".")
    }

  private def fingerprint(dataset: String, name: String): List[String] =
    run(
      s"SELECT COUNT(*), BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) FROM `$dataset.$name` t"
    ).iterateAll()
      .asScala
      .map(row => (0 until 2).map(i => row.get(i).getStringValue).toList)
      .toList
      .flatten

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "A BigQuery table extracted then loaded natively" should "keep its schema and values" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        List(sourceDataset, targetDataset).foreach { dataset =>
          if (bigquery.getDataset(dataset) == null)
            bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
        }
        List(table, ranges).foreach(name => bigquery.delete(TableId.of(targetDataset, name)))
        run(s"""CREATE OR REPLACE TABLE `$sourceDataset.$table` (
               |  id INT64 NOT NULL,
               |  civil DATETIME,
               |  wall TIME,
               |  payload BYTES,
               |  big BIGNUMERIC,
               |  price NUMERIC,
               |  area GEOGRAPHY,
               |  duration INTERVAL,
               |  doc JSON,
               |  tags ARRAY<STRING>,
               |  record STRUCT<instant TIMESTAMP, civils ARRAY<DATETIME>>
               |)""".stripMargin)
        run(s"""INSERT INTO `$sourceDataset.$table` VALUES
               |(1, DATETIME '2019-12-31 23:59:59.999999', TIME '23:59:59.999999', b'\\x00\\xff\\x10',
               | BIGNUMERIC '1234567890123456789012345678901234567.12345678901234567890123456789012345678',
               | NUMERIC '-12345678901234567890123456789.123456789', ST_GEOGPOINT(1, 2),
               | INTERVAL '1-2 3 4:5:6.789' YEAR TO SECOND, JSON '{"a": [1, "b"]}', ['x', 'y'],
               | STRUCT(TIMESTAMP '2019-12-31 23:59:59.999999 UTC', [DATETIME '2019-01-01 00:00:00'])),
               |(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, [], NULL)""".stripMargin)
        run(s"""CREATE OR REPLACE TABLE `$sourceDataset.$ranges` (
               |  id INT64 NOT NULL,
               |  days RANGE<DATE>,
               |  civils RANGE<DATETIME>,
               |  instants RANGE<TIMESTAMP>
               |)""".stripMargin)
        run(s"""INSERT INTO `$sourceDataset.$ranges` VALUES
               |(1, RANGE(DATE '2019-01-01', DATE '2019-12-31'),
               | RANGE(DATETIME '2019-01-01 10:00:00.5', NULL),
               | RANGE(TIMESTAMP '2019-01-01 00:00:00 UTC', TIMESTAMP '2019-12-31 23:59:59.999999 UTC')),
               |(2, NULL, NULL, NULL)""".stripMargin)
        run(
          s"CREATE OR REPLACE VIEW `$sourceDataset.$view` AS SELECT * FROM `$sourceDataset.$table`"
        )

        val extracted = new ExtractBigQuerySchema(
          TablesExtractConfig(
            connectionRef = Some("bigquery"),
            tables = Map(sourceDataset -> List(table, view, ranges)),
            tableTypes = List("TABLE", "VIEW")
          )
        ).extractSchemasAndTables(
          settings.schemaHandler(),
          Map(sourceDataset -> List(table, view, ranges))
        ).head
        val types = extracted.tables
          .map(schema => schema.name -> schema.attributes.map(a => a.name -> a.`type`))
          .toMap
        types(table) shouldBe List(
          "id"       -> "long",
          "civil"    -> "datetime",
          "wall"     -> "time",
          "payload"  -> "binary",
          "big"      -> "bignumeric",
          "price"    -> "decimal",
          "area"     -> "geography",
          "duration" -> "interval",
          "doc"      -> "variant",
          "tags"     -> "string",
          "record"   -> "struct"
        )
        // The effective schema of the view
        types(view) shouldBe types(table)
        types(ranges) shouldBe List(
          "id"       -> "long",
          "days"     -> "range_date",
          "civils"   -> "range_datetime",
          "instants" -> "range_timestamp"
        )

        // The extracted table, loaded into the target dataset from newline delimited JSON
        extracted
          .copy(
            name = targetDataset,
            database = None,
            metadata = Some(
              Metadata(
                format = Some(Format.JSON),
                directory = Some(s"{{root_path}}/incoming/$targetDataset")
              )
            ),
            tables = extracted.tables.collect {
              case schema if schema.name == table =>
                schema.copy(pattern = Pattern.compile(s"$table-.*\\.json"))
              case schema if schema.name == ranges =>
                schema.copy(
                  pattern = Pattern.compile(s"$ranges-.*\\.csv"),
                  metadata = Some(
                    Metadata(
                      format = Some(Format.DSV),
                      withHeader = Some(true),
                      separator = Some(";")
                    )
                  )
                )
            }
          )
          .writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())
        val incoming = (project / "incoming" / targetDataset).createDirectories()
        // GEOGRAPHY as its WKT text, and DATETIME with a space, as JSON loads read them. INTERVAL
        // is written in ISO 8601, as JSON loads read it
        val datetimeFormat = "'%F %H:%M:%E6S'"
        val lines = run(
          s"""SELECT TO_JSON_STRING(t) FROM (
             |  SELECT * REPLACE (
             |    ST_ASTEXT(area) AS area,
             |    FORMAT_DATETIME($datetimeFormat, civil) AS civil,
             |    IF(record IS NULL, NULL, STRUCT(record.instant AS instant, ARRAY(
             |      SELECT FORMAT_DATETIME($datetimeFormat, c) FROM UNNEST(record.civils) c WITH OFFSET o
             |      ORDER BY o) AS civils)) AS record)
             |  FROM `$sourceDataset.$table`) t""".stripMargin
        ).iterateAll().asScala.map(_.get(0).getStringValue).toList
        (incoming / s"$table-1.json").overwrite(lines.mkString("", "\n", "\n"))
        // The canonical text of the RANGE values, as CSV loads read them
        val rangeLines = run(
          s"""SELECT CONCAT(CAST(id AS STRING), ';', IFNULL(CAST(days AS STRING), ''), ';',
             |  IFNULL(CAST(civils AS STRING), ''), ';', IFNULL(CAST(instants AS STRING), ''))
             |FROM `$sourceDataset.$ranges` ORDER BY id""".stripMargin
        ).iterateAll().asScala.map(_.get(0).getStringValue).toList
        (incoming / s"$ranges-1.csv").overwrite(
          ("id;days;civils;instants" :: rangeLines).mkString("", "\n", "\n")
        )
        assert(new Main().run(Array("stage")))
        assert(new Main().run(Array("load")))

        List(table, ranges).foreach { name =>
          describe(fields(targetDataset, name)) shouldBe describe(fields(sourceDataset, name))
          fingerprint(targetDataset, name) shouldBe fingerprint(sourceDataset, name)
        }
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    project.delete(swallowIOExceptions = true)
  }
}
