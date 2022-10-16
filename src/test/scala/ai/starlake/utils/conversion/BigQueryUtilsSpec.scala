package ai.starlake.utils.conversion

import ai.starlake.TestHelper
import ai.starlake.config.SparkEnv
import ai.starlake.schema.handlers.SchemaHandler
import com.google.cloud.bigquery.{Field, Schema => BQSchema, StandardSQLTypeName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class BigQueryUtilsSpec extends TestHelper {
  new WithSettings() {
    val sparkEnv: SparkEnv = new SparkEnv("test")
    val session: SparkSession = sparkEnv.session

    "Spark Types" should "be converted to corresponding BQ Types" in {
      val sparkSchema = StructType(
        Seq(
          StructField("categoryId", StringType, nullable = true),
          StructField(
            "categorySynonyms",
            ArrayType(StringType, containsNull = true),
            nullable = true
          ),
          StructField("isNew", BooleanType, nullable = true),
          StructField("exclusiveOfferCode", IntegerType, nullable = true),
          StructField(
            "filters",
            ArrayType(
              StructType(
                Seq(
                  StructField("dimension", StringType, nullable = false),
                  StructField(
                    "dimensionValues",
                    ArrayType(
                      StructType(
                        Seq(
                          StructField("identifier", StringType, nullable = true),
                          StructField("label", StringType, nullable = false)
                        )
                      ),
                      containsNull = true
                    ),
                    nullable = true
                  ),
                  StructField("name", StringType, nullable = true)
                )
              ),
              containsNull = true
            ),
            nullable = true
          ),
          StructField("name", StringType, nullable = false),
          StructField("subCategories", ArrayType(BinaryType, containsNull = true), nullable = true)
        )
      )
      // Schema{fields=[Field{name=value, type=INTEGER, mode=NULLABLE, description=, policyTags=null}]}
      val bqSchemaExpected = BQSchema.of(
        Field
          .newBuilder("categoryId", StandardSQLTypeName.STRING)
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("categorySynonyms", StandardSQLTypeName.STRING)
          .setMode(Field.Mode.REPEATED)
          .build(),
        Field
          .newBuilder("isNew", StandardSQLTypeName.BOOL)
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("exclusiveOfferCode", StandardSQLTypeName.INT64)
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder(
            "filters",
            StandardSQLTypeName.STRUCT,
            Field
              .newBuilder("dimension", StandardSQLTypeName.STRING)
              .setMode(Field.Mode.REQUIRED)
              .build(),
            Field
              .newBuilder(
                "dimensionValues",
                StandardSQLTypeName.STRUCT,
                Field
                  .newBuilder("identifier", StandardSQLTypeName.STRING)
                  .setMode(Field.Mode.NULLABLE)
                  .build(),
                Field
                  .newBuilder("label", StandardSQLTypeName.STRING)
                  .setMode(Field.Mode.REQUIRED)
                  .build()
              )
              .setMode(Field.Mode.REPEATED)
              .build(),
            Field
              .newBuilder("name", StandardSQLTypeName.STRING)
              .setMode(Field.Mode.NULLABLE)
              .build()
          )
          .setMode(Field.Mode.REPEATED)
          .build(),
        Field
          .newBuilder("name", StandardSQLTypeName.STRING)
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("subCategories", StandardSQLTypeName.BYTES)
          .setMode(Field.Mode.REPEATED)
          .build()
      )

      BigQueryUtils.bqSchema(sparkSchema) shouldBe bqSchemaExpected
    }

    "Schema" should "return the right bq schema" in {

      new SpecTrait(
        domainOrJobFilename = "DOMAIN.comet.yml",
        sourceDomainOrJobPathname = s"/sample/DOMAIN.comet.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
      ) {

        cleanMetadata
        cleanDatasets

        val schemaHandler = new SchemaHandler(settings.storageHandler)

        val schema = schemaHandler
          .domains()
          .flatMap(_.tables)
          .find(_.name == "User")
          .map(_.bqSchema(schemaHandler))

        val bqSchemaExpected = BQSchema.of(
          Field
            .newBuilder("firstname", StandardSQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .setDescription("")
            .build(),
          Field
            .newBuilder("lastname", StandardSQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .setDescription("")
            .build(),
          Field
            .newBuilder("age", StandardSQLTypeName.INT64)
            .setMode(Field.Mode.NULLABLE)
            .setDescription("")
            .build(),
          Field
            .newBuilder("ok", StandardSQLTypeName.BOOL)
            .setMode(Field.Mode.NULLABLE)
            .setDescription("")
            .build()
        )

        schema shouldBe Some(bqSchemaExpected)

      }

    }

  }
}
