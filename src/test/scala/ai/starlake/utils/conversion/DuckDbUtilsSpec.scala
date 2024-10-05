package ai.starlake.utils.conversion

import ai.starlake.TestHelper
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

class DuckDbUtilsSpec extends TestHelper {
  new WithSettings() {
    "Spark Types" should "be converted to corresponding DuckDB table 1" in {
      val sparkSchema =
        StructType(
          Seq(
            StructField(
              "glossary",
              StructType(
                Seq(
                  StructField("title", StringType, nullable = true),
                  StructField(
                    "GlossDiv",
                    StructType(
                      Seq(
                        StructField("title", StringType, nullable = true),
                        StructField(
                          "GlossList",
                          StructType(
                            Seq(
                              StructField(
                                "GlossEntry",
                                StructType(
                                  Seq(
                                    StructField("ID", StringType, nullable = true),
                                    StructField("SortAs", StringType, nullable = true),
                                    StructField("GlossTerm", StringType, nullable = true),
                                    StructField("Acronym", StringType, nullable = true),
                                    StructField("Abbrev", StringType, nullable = true),
                                    StructField(
                                      "GlossDef",
                                      StructType(
                                        Seq(
                                          StructField("para", StringType, nullable = true),
                                          StructField(
                                            "GlossSeeAlso",
                                            ArrayType(StringType, containsNull = true),
                                            nullable = true
                                          ),
                                          StructField(
                                            "IntArray",
                                            ArrayType(LongType, containsNull = true),
                                            nullable = true
                                          )
                                        )
                                      ),
                                      nullable = true
                                    ),
                                    StructField("GlossSee", StringType, nullable = true)
                                  )
                                ),
                                nullable = true
                              )
                            )
                          ),
                          nullable = true
                        )
                      )
                    ),
                    nullable = true
                  )
                )
              ),
              nullable = true
            )
          )
        )
      val result = DuckDbUtils.bqSchema(sparkSchema)
      val expected = Array(
        "glossary STRUCT(title VARCHAR, GlossDiv STRUCT(title VARCHAR, GlossList STRUCT(GlossEntry STRUCT(ID VARCHAR, SortAs VARCHAR, GlossTerm VARCHAR, Acronym VARCHAR, Abbrev VARCHAR, GlossDef STRUCT(para VARCHAR, GlossSeeAlso VARCHAR[], IntArray BIGINT[]), GlossSee VARCHAR))))"
      )
      println(result.mkString(","))
      assert(result === expected)
    }
  }
  "Spark Schema " should "be converted to the correct DuckDB table 2" in {
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
    val result = DuckDbUtils.bqSchema(sparkSchema)
    val expected = Array(
      "categoryId VARCHAR",
      "categorySynonyms VARCHAR[]",
      "isNew BOOLEAN",
      "exclusiveOfferCode INTEGER",
      "filters STRUCT(dimension VARCHAR, dimensionValues STRUCT(identifier VARCHAR, label VARCHAR)[], name VARCHAR)[]",
      "name VARCHAR NOT NULL",
      "subCategories BINARY[]"
    )
    println(result.mkString(","))
    assert(result === expected)
  }
}
