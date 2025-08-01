package ai.starlake.utils.conversion

import ai.starlake.TestHelper
import ai.starlake.config.SparkEnv
import com.google.cloud.bigquery.{Field, FieldList, Schema => BQSchema, StandardSQLTypeName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class BigQueryUtilsSpec extends TestHelper {
  new WithSettings() {
    val sparkEnv: SparkEnv = SparkEnv.get("test", identity, settings)
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
        sourceDomainOrJobPathname = s"/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
      ) {

        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/User.sl.yml",
          "/sample/Players.sl.yml",
          "/sample/employee.sl.yml",
          "/sample/complexUser.sl.yml"
        ).foreach(deliverSourceTable)

        val schemaHandler = settings.schemaHandler()

        val schema = schemaHandler
          .domains()
          .flatMap(_.tables)
          .find(_.name == "complexUser")
          .map(_.bigquerySchemaWithoutIgnore(schemaHandler, withFinalName = true))

        val bqSchemaExpected = BQSchema.of(
          Field
            .newBuilder("firstname", StandardSQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .setDescription("first name comment")
            .build(),
          Field
            .newBuilder("lastname", StandardSQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .setDescription("last name comment")
            .build(),
          Field
            .newBuilder("age", StandardSQLTypeName.INT64)
            .setMode(Field.Mode.NULLABLE)
            .setDescription("age comment")
            .build(),
          Field
            .newBuilder(
              "familySituation",
              StandardSQLTypeName.STRUCT,
              FieldList.of(
                Field
                  .newBuilder(
                    "children",
                    StandardSQLTypeName.STRUCT,
                    FieldList.of(
                      Field
                        .newBuilder("firstName", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.NULLABLE)
                        .setDescription("child first name comment")
                        .build()
                    )
                  )
                  .setMode(Field.Mode.REPEATED)
                  .setDescription("children comment")
                  .build(),
                Field
                  .newBuilder("married", StandardSQLTypeName.BOOL)
                  .setMode(Field.Mode.NULLABLE)
                  .setDescription("married comment")
                  .build()
              )
            )
            .setMode(Field.Mode.NULLABLE)
            .setDescription("family situation comment")
            .build()
        )

        schema shouldBe Some(bqSchemaExpected)

      }

    }

  }
}
