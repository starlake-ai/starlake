package ai.starlake.job.validator

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.job.validator.RowValidator.SL_ERROR_COL
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import org.apache.spark.sql.functions.{array_size, col, explode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll

import java.time.format.DateTimeFormatter
import java.time.{OffsetDateTime, ZoneId}
import scala.jdk.CollectionConverters._

class RowValidatorSpec extends TestHelper with BeforeAndAfterAll {
  val complexStructure = List(
    Attribute(
      "id",
      PrimitiveType.int.value,
      array = Some(false),
      required = Some(true),
      privacy = Some(TransformInput.None),
      trim = Some(Trim.BOTH)
    ),
    Attribute(
      "temperature",
      PrimitiveType.double.value,
      array = Some(false),
      required = Some(false),
      privacy = Some(TransformInput.None),
      default = Some("999.99")
    ),
    Attribute(
      "user_info",
      PrimitiveType.struct.value,
      array = Some(false),
      required = Some(false),
      privacy = Some(TransformInput.None),
      attributes = List(
        Attribute(
          "firstname",
          PrimitiveType.string.value,
          array = Some(false),
          required = Some(false),
          privacy = Some(TransformInput.None),
          trim = Some(Trim.BOTH),
          default = Some(" UNKNOWN UNTRIMMED ")
        ),
        Attribute(
          "lastname",
          PrimitiveType.string.value,
          array = Some(false),
          required = Some(false),
          privacy = Some(TransformInput("SHA1", sql = false))
        )
      )
    ),
    Attribute(
      "geojson",
      PrimitiveType.variant.value,
      array = Some(false),
      required = Some(false),
      privacy = None
    ),
    Attribute(
      "address",
      PrimitiveType.string.value,
      array = Some(true),
      required = Some(false),
      privacy = None,
      attributes = List(
        Attribute(
          "addressId",
          PrimitiveType.int.value,
          array = Some(false),
          required = Some(true),
          privacy = None,
          trim = Some(Trim.BOTH),
          default = Some("-1")
        ),
        Attribute(
          "type",
          PrimitiveType.string.value,
          array = Some(false),
          required = Some(true),
          privacy = None
        ),
        Attribute(
          "full_address",
          PrimitiveType.string.value,
          array = Some(false),
          required = Some(false),
          privacy = None,
          default = Some("N/A")
        )
      )
    )
  )

  val validTypeMatchData = List(
    """{"id": 1,
      |"temperature": 25.5,
      |"user_info": {
      |  "firstname": "regular-json",
      |  "lastname": "sensible-data"
      |},
      |"geojson": "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,10],[10,10],[10,0],[0,0]],[[3,3],[7,3],[7,7],[3,7],[3,3]],[[0.5,0.5],[1.5,0.5],[1.5,1.5],[0.5,1.5],[0.5,0.5]]]}}]}",
      |"address": [
      |  {
      |    "addressId": 1,
      |    "type": "home",
      |    "full_address": "1 home street"
      |  },
      |  {
      |    "addressId": 2,
      |    "type": "office",
      |    "full_address": "1 office street"
      |  }
      |]
      |}""".stripMargin,
    """{"id": 2,
      |"temperature": 25.5,
      |"user_info": {
      |  "firstname": " to-trim-both-json ",
      |  "lastname": "sensible-data"
      |},
      |"geojson": "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,10],[10,10],[10,0],[0,0]],[[3,3],[7,3],[7,7],[3,7],[3,3]],[[0.5,0.5],[1.5,0.5],[1.5,1.5],[0.5,1.5],[0.5,0.5]]]}}]}",
      |"address": [
      |  {
      |    "type": "home",
      |    "full_address": "1 home street"
      |  }
      |]
      |}""".stripMargin,
    """{"id": 3,
      |"user_info": {
      |  "lastname": "sensible-data"
      |},
      |"geojson": "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,10],[10,10],[10,0],[0,0]],[[3,3],[7,3],[7,7],[3,7],[3,3]],[[0.5,0.5],[1.5,0.5],[1.5,1.5],[0.5,1.5],[0.5,0.5]]]}}]}",
      |"address": [
      |  {
      |    "type": "home",
      |    "full_address": "1 home street"
      |  }
      |]
      |}""".stripMargin,
    """{"id": 4,
      |"user_info": {},
      |"address": [
      |  {
      |    "addressId": "1",
      |    "full_address": ""
      |  },
      |  {
      |    "addressId": ""
      |  }
      |]
      |}""".stripMargin
  )

  "prepareData" should "trim and apply default value" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      val spark = sparkSession
      import spark.implicits._
      val rowValidatorEmptyIsNull =
        new RowValidator(
          complexStructure,
          settings.schemaHandler().types(),
          Map.empty,
          emptyIsNull = true,
          rejectWithValue = true
        )
      val df = spark.read.json(sparkSession.createDataset(validTypeMatchData))
      val preparedDF =
        df.transform(rowValidatorEmptyIsNull.prepareData(Attributes.from(df.schema)))
      preparedDF.schema.fields.exists(_.name == RowValidator.SL_INPUT_COL) shouldBe true
      private val outputSchema: StructType =
        makeFieldsNullable(preparedDF.drop(RowValidator.SL_INPUT_COL).schema)
      outputSchema shouldBe StructType(
        List(
          StructField("id", DataTypes.StringType, true),
          StructField("temperature", DataTypes.DoubleType, true),
          StructField(
            "user_info",
            StructType(
              List(
                StructField("firstname", DataTypes.StringType, true),
                StructField("lastname", DataTypes.StringType, true)
              )
            ),
            true
          ),
          StructField("geojson", DataTypes.StringType, true),
          StructField(
            "address",
            ArrayType(
              StructType(
                List(
                  StructField("addressId", DataTypes.StringType, true),
                  StructField("type", DataTypes.StringType, true),
                  StructField("full_address", DataTypes.StringType, true)
                )
              ),
              true
            ),
            true
          )
        )
      )

      val expectedRow1Output = mapper
        .createObjectNode()
      expectedRow1Output
        .put("id", "1")
        .put("temperature", 25.5d)
        .put(
          "geojson",
          "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,10],[10,10],[10,0],[0,0]],[[3,3],[7,3],[7,7],[3,7],[3,3]],[[0.5,0.5],[1.5,0.5],[1.5,1.5],[0.5,1.5],[0.5,0.5]]]}}]}"
        )
        .putObject("user_info")
        .put("firstname", "regular-json")
        .put("lastname", "sensible-data")
      expectedRow1Output
        .putArray("address")
        .add(
          mapper
            .createObjectNode()
            .put("addressId", "1")
            .put("type", "home")
            .put("full_address", "1 home street")
        )
        .add(
          mapper
            .createObjectNode()
            .put("addressId", "2")
            .put("type", "office")
            .put("full_address", "1 office street")
        )
      val expectedRow2Output = mapper.createObjectNode()
      expectedRow2Output
        .put("id", "2")
        .put("temperature", 25.5d)
        .put(
          "geojson",
          "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,10],[10,10],[10,0],[0,0]],[[3,3],[7,3],[7,7],[3,7],[3,3]],[[0.5,0.5],[1.5,0.5],[1.5,1.5],[0.5,1.5],[0.5,0.5]]]}}]}"
        )
        .putObject("user_info")
        .put("firstname", "to-trim-both-json")
        .put("lastname", "sensible-data")
      expectedRow2Output
        .putArray("address")
        .add(
          mapper
            .createObjectNode()
            .put("addressId", "-1")
            .put("type", "home")
            .put("full_address", "1 home street")
        )

      val expectedRow3Output = mapper
        .createObjectNode()
        .put("id", "3")
        .put("temperature", 999.99d)
        .put(
          "geojson",
          "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,10],[10,10],[10,0],[0,0]],[[3,3],[7,3],[7,7],[3,7],[3,3]],[[0.5,0.5],[1.5,0.5],[1.5,1.5],[0.5,1.5],[0.5,0.5]]]}}]}"
        )
      expectedRow3Output
        .putObject("user_info")
        .put("firstname", " UNKNOWN UNTRIMMED ")
        .put("lastname", "sensible-data")
      expectedRow3Output
        .putArray("address")
        .add(
          mapper
            .createObjectNode()
            .put("addressId", "-1")
            .put("type", "home")
            .put("full_address", "1 home street")
        )

      val expectedRow4Output = mapper
        .createObjectNode()
        .put("id", "4")
        .put("temperature", 999.99d)

      expectedRow4Output
        .putObject("user_info")
        .put("firstname", " UNKNOWN UNTRIMMED ")
      expectedRow4Output
        .putArray("address")
        .add(
          mapper
            .createObjectNode()
            .put("addressId", "1")
            .put("full_address", "N/A")
        )
        .add(
          mapper
            .createObjectNode()
            .put("addressId", "-1")
            .put("full_address", "N/A")
        )

      val expectedRow4EmptyNotNullOutput = mapper
        .createObjectNode()
        .put("id", "4")
        .put("temperature", 999.99d)
      expectedRow4EmptyNotNullOutput
        .putObject("user_info")
        .put("firstname", " UNKNOWN UNTRIMMED ")
      expectedRow4EmptyNotNullOutput
        .putArray("address")
        .add(
          mapper
            .createObjectNode()
            .put("addressId", "1")
            .put("full_address", "")
        )
        .add(
          mapper
            .createObjectNode()
            .put("addressId", "")
            .put("full_address", "N/A")
        )

      preparedDF
        .drop(RowValidator.SL_INPUT_COL)
        .toJSON
        .collect()
        .toList
        .map(mapper.readTree) should contain theSameElementsAs List(
        expectedRow1Output,
        expectedRow2Output,
        expectedRow3Output,
        expectedRow4Output
      )
      val rowValidatorEmptyIsNotNull =
        new RowValidator(
          complexStructure,
          settings.schemaHandler().types(),
          Map.empty,
          emptyIsNull = false,
          rejectWithValue = true
        )
      val preparedDF2 =
        df.transform(rowValidatorEmptyIsNotNull.prepareData(Attributes.from(df.schema)))
      preparedDF2
        .drop(RowValidator.SL_INPUT_COL)
        .toJSON
        .collect()
        .toList
        .map(mapper.readTree) should contain theSameElementsAs List(
        expectedRow1Output,
        expectedRow2Output,
        expectedRow3Output,
        expectedRow4EmptyNotNullOutput
      )
    }
  }

  it should "ignore unmatched type of array or struct" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      val spark = sparkSession
      val inputDF =
        spark.sql("select 1 as should_be_array, array(1) as should_be_array_of_struct").toDF()
      val rowValidator =
        new RowValidator(
          List(
            Attribute(
              name = "should_be_array",
              `type` = PrimitiveType.string.value,
              array = Some(true)
            ),
            Attribute(
              name = "should_be_array_of_struct",
              `type` = PrimitiveType.struct.value,
              array = Some(true),
              attributes =
                List(Attribute(name = "struct_field", `type` = PrimitiveType.string.value))
            )
          ),
          settings.schemaHandler().types(),
          Map.empty,
          emptyIsNull = false,
          rejectWithValue = true
        )
      val expectedOutput = mapper.createObjectNode()
      expectedOutput
        .putArray("should_be_array_of_struct")
      inputDF
        .transform(
          rowValidator
            .prepareData(Attributes.from(inputDF.schema))
        )
        .drop(RowValidator.SL_INPUT_COL)
        .toJSON
        .collect()
        .toList
        .map(mapper.readTree) should contain theSameElementsAs List(
        expectedOutput
      )
    }
  }

  it should "ignore missing fields and drop them" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      val spark = sparkSession
      val rowValidator =
        new RowValidator(
          List(
            Attribute(
              name = "should_be_array",
              `type` = PrimitiveType.string.value,
              array = Some(true)
            ),
            Attribute(
              name = "should_be_array_of_struct",
              `type` = PrimitiveType.struct.value,
              array = Some(true),
              attributes =
                List(Attribute(name = "struct_field", `type` = PrimitiveType.string.value))
            )
          ),
          settings.schemaHandler().types(),
          Map.empty,
          emptyIsNull = false,
          rejectWithValue = true
        )
      val inputDF =
        spark
          .sql(
            "select 1 as extra_column, array(named_struct('a', 1)) as should_be_array, array(named_struct('a', 1)) as should_be_array_of_struct"
          )
          .toDF()
      val expectedOutput = mapper.createObjectNode()
      expectedOutput
        .putArray("should_be_array")
      expectedOutput.putArray("should_be_array_of_struct").add(mapper.createObjectNode())
      inputDF
        .transform(
          rowValidator
            .prepareData(Attributes.from(inputDF.schema))
        )
        .drop(RowValidator.SL_INPUT_COL)
        .toJSON
        .collect()
        .toList
        .map(mapper.readTree) should contain theSameElementsAs List(
        expectedOutput
      )

      val inputDF2 =
        spark
          .sql(
            "select 1 as extra_column, array(2) as should_be_array"
          )
          .toDF()
      val expectedOutput2 = mapper.createObjectNode()
      expectedOutput2
        .putArray("should_be_array")
        .add(2)
      inputDF2
        .transform(
          rowValidator
            .prepareData(Attributes.from(inputDF2.schema))
        )
        .drop(RowValidator.SL_INPUT_COL)
        .toJSON
        .collect()
        .toList
        .map(mapper.readTree) should contain theSameElementsAs List(
        expectedOutput2
      )
    }
  }

  private val allPrimitiveTypes =
    PrimitiveType.primitiveTypes.toList.filter(_.value != PrimitiveType.struct.value)

  def assertValidateFun(
    types: List[Type],
    inputRows: Seq[Row],
    inputSchema: StructType,
    ingestionSchema: List[Attribute]
  )(
    assertionRules: DataFrame => Unit
  )(implicit spark: SparkSession, schemaHandler: SchemaHandler): Unit = {
    val df = spark.createDataFrame(
      inputRows.asJava,
      inputSchema
    )
    assertValidateFun(types, df, ingestionSchema)(assertionRules)
  }

  def assertValidateFun(
    types: List[Type],
    inputDF: DataFrame,
    ingestionSchema: List[Attribute]
  )(
    assertionRules: DataFrame => Unit
  )(implicit schemaHandler: SchemaHandler): Unit = {
    val rowValidator = new RowValidator(ingestionSchema, types, Map.empty, false, true)
    val validatedDF = inputDF.transform(rowValidator.validate(Attributes.from(inputDF.schema)))
    assertionRules(validatedDF)
  }

  def assertSchemaFitFun(
    types: List[Type],
    inputRows: Seq[Row],
    inputSchema: StructType,
    ingestionSchema: List[Attribute]
  )(
    assertionRules: DataFrame => Unit
  )(implicit spark: SparkSession, schemaHandler: SchemaHandler): Unit = {
    val df = spark.createDataFrame(
      inputRows.asJava,
      inputSchema
    )
    assertSchemaFitFun(types, df, ingestionSchema)(assertionRules)
  }

  def assertSchemaFitFun(
    types: List[Type],
    inputDF: DataFrame,
    ingestionSchema: List[Attribute]
  )(
    assertionRules: DataFrame => Unit
  )(implicit schemaHandler: SchemaHandler): Unit = {
    val rowValidator = new RowValidator(ingestionSchema, types, Map.empty, false, true)
    val validatedDF = inputDF.transform(rowValidator.fitToSchema(Attributes.from(inputDF.schema)))
    assertionRules(validatedDF)
  }

  def defaultTypes(implicit settings: Settings): List[Type] = {
    settings.schemaHandler().types()
  }

  def overridedTypesForInvalidTests(implicit settings: Settings): List[Type] = {
    defaultTypes.filter(_.name != "string") :+ Type(
      name = PrimitiveType.string.value,
      primitiveType = PrimitiveType.string,
      pattern = ".*CONTAINS.*"
    )
  }

  def primitiveNativeType(): (Row, StructType, List[Attribute]) = {
    val nonEmptyRow = allPrimitiveTypes.map {
      case PrimitiveType.string  => "string value"
      case PrimitiveType.variant => "{}"
      case PrimitiveType.long    => 10L
      case PrimitiveType.int     => 11
      case PrimitiveType.short   => 12.toShort
      case PrimitiveType.double  => 12.5
      case PrimitiveType.decimal => BigDecimal(12.5)
      case PrimitiveType.boolean => true
      case PrimitiveType.byte    => 1.toByte
      case PrimitiveType.struct =>
        throw new RuntimeException("Should not happen since it is filtered out")
      case PrimitiveType.date => java.sql.Date.valueOf("2023-10-27")
      case PrimitiveType.timestamp =>
        java.sql.Timestamp.valueOf(
          OffsetDateTime
            .parse("2011-12-03T10:15:30+05:00")
            .atZoneSameInstant(ZoneId.systemDefault())
            .toLocalDateTime
        )
    }
    // Create sample data for each primitive type
    val inputRow = Row(
      nonEmptyRow: _*
    )

    // Define column names based on attribute names
    val columnNames = allPrimitiveTypes.map(_.value.toLowerCase)
    val inputSchema = StructType(
      columnNames.map(name =>
        StructField(
          name,
          allPrimitiveTypes.find(_.value == name).map(_.sparkType(None)).get,
          true
        )
      )
    )

    val ingestionSchema = allPrimitiveTypes.map { primitiveType =>
      Attribute(name = primitiveType.toString.toLowerCase).copy(
        `type` = primitiveType match {
          case PrimitiveType.timestamp => "iso_date_time"
          case _                       => primitiveType.toString
        },
        required = Some(false)
      )
    }
    (inputRow, inputSchema, ingestionSchema)
  }

  def nullifyRowLeaves(inputRow: Row): Row = {
    Row((0 until inputRow.size).map { i =>
      inputRow.get(i) match {
        case r: Row => nullifyRowLeaves(r)
        case l: Seq[_] =>
          l.map {
            case r: Row        => nullifyRowLeaves(r)
            case _: Seq[_] | _ => null
            // Starlake don't allow to have arrays of arrays so it should be a field with variant type
          }
        case _ => null
      }
    }: _*)
  }

  def primitiveStringType(): (Row, StructType, List[Attribute]) = {
    val (inputRow, inputSchema, ingestionSchema) = primitiveNativeType()

    val stringInputRow = Row((0 until inputRow.size).map { i =>
      inputRow.get(i) match {
        case t: java.sql.Timestamp =>
          val localDateTime = t.toLocalDateTime
          val offsetDateTime = localDateTime.atZone(ZoneId.systemDefault()).toOffsetDateTime()
          offsetDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        case e => String.valueOf(e)
      }
    }: _*)
    val stringInputSchema =
      inputSchema.copy(fields = inputSchema.fields.map(_.copy(dataType = StringType)))
    (stringInputRow, stringInputSchema, ingestionSchema)
  }

  def invalidPrimitiveStringType(): (Row, StructType, List[Attribute]) = {
    val (_, inputSchema, ingestionSchema) = primitiveStringType()

    val nonEmptyRow = allPrimitiveTypes.map {
      case PrimitiveType.string =>
        "string value\nthis is not the expected keyword: MISSING in \n the pattern."
      case PrimitiveType.variant => "{}"
      case PrimitiveType.long    => "a10"
      case PrimitiveType.int =>
        "9223372036854775808" // Primitive type doesn't have java int in our types so we try to overflow long
      case PrimitiveType.short   => "+"
      case PrimitiveType.double  => "12,5"
      case PrimitiveType.decimal => "012.5F-5"
      case PrimitiveType.boolean => "vrai"
      case PrimitiveType.byte    => "more than one byte"
      case PrimitiveType.struct =>
        throw new RuntimeException("Should not happen since it is filtered out")
      case PrimitiveType.date      => "2023-13-27"
      case PrimitiveType.timestamp => "2011-12-03 10:15:30+01:00"
    }
    (Row(nonEmptyRow: _*), inputSchema, ingestionSchema)
  }

  "validate" should "validate all primitive types with string type" in {
    new WithSettings() {
      implicit val schemaHandler = settings.schemaHandler()
      implicit val spark = sparkSession
      // Create attributes for each primitive type

      val (inputRow, inputSchema, ingestionSchema) = primitiveStringType()
      assertValidateFun(
        defaultTypes,
        List(inputRow, nullifyRowLeaves(inputRow)),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) === 0).count() shouldBe 2
      }
    }
  }

  it should "validate all primitive types with native type" in {
    // allows to support files such as parquet where timestamp or date are already defined
    new WithSettings() {
      implicit val schemaHandler = settings.schemaHandler()
      implicit val spark = sparkSession

      val (inputRow, inputSchema, ingestionSchema) = primitiveNativeType()
      assertValidateFun(
        defaultTypes,
        List(inputRow, nullifyRowLeaves(inputRow)),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) === 0).count() shouldBe 2
      }
    }
  }

  it should "reject when pattern is invalid or overflow" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      implicit val spark: SparkSession = sparkSession
      import spark.implicits._

      val (inputRow, inputSchema, ingestionSchema) = invalidPrimitiveStringType()
      assertValidateFun(
        overridedTypesForInvalidTests,
        List(inputRow),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) > 0).count() shouldBe 1
        val errors: Array[String] =
          validatedDF.select(explode(col(SL_ERROR_COL))).as[String].collect()

        errors should contain theSameElementsAs List(
          "`timestamp` (iso_date_time) doesn't match pattern `ISO_DATE_TIME`: 2011-12-03 10:15:30+01:00",
          "`date` (date) doesn't match pattern `yyyy-MM-dd`: 2023-13-27",
          "`long` (long) doesn't match pattern `[-+]?[0-9]+`: a10",
          "`int` cannot be converted to `long` with sl type `int`: 9223372036854775808",
          "`double` (double) doesn't match pattern `[-+]?\\d*\\.?\\d+[Ee]?[-+]?\\d*`: 12,5",
          "`decimal` (decimal) doesn't match pattern `[+-]?\\d*\\.?\\d+(?:[eE][+-]?\\d+)?`: 012.5F-5",
          "`byte` (byte) doesn't match pattern `.`: more than one byte",
          "`short` (short) doesn't match pattern `[-+]?[0-9]+`: +",
          "`string` (string) doesn't match pattern `.*CONTAINS.*`: string value\nthis is not the expected keyword: MISSING in \n the pattern.",
          "`boolean` (boolean) doesn't match pattern `(?i)true|yes|[y1]|t<-TF->(?i)false|no|[n0]|f`: vrai"
        )
      }
    }
  }

  it should "validate all primitive types with string type within an array" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      implicit val spark: SparkSession = sparkSession

      val (primitiveInputRow, primitiveInputSchema, primitiveIngestionSchema) =
        primitiveStringType()
      val inputRow =
        Row((0 until primitiveInputRow.size).map(i => List(primitiveInputRow.get(i))): _*)
      val inputSchema = primitiveInputSchema.copy(fields =
        primitiveInputSchema.fields.map(f => f.copy(dataType = ArrayType(f.dataType)))
      )
      val ingestionSchema = primitiveIngestionSchema.map(_.copy(array = Some(true)))

      assertValidateFun(
        defaultTypes,
        List(inputRow, nullifyRowLeaves(inputRow)),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) === 0).count() shouldBe 2
      }
    }
  }

  it should "reject when pattern is invalid or overflow within an array" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      implicit val spark: SparkSession = sparkSession
      import spark.implicits._

      val (primitiveInputRow, primitiveInputSchema, primitiveIngestionSchema) =
        invalidPrimitiveStringType()
      val inputRow =
        Row((0 until primitiveInputRow.size).map(i => List(primitiveInputRow.get(i))): _*)
      val inputSchema = primitiveInputSchema.copy(fields =
        primitiveInputSchema.fields.map(f => f.copy(dataType = ArrayType(f.dataType)))
      )
      val ingestionSchema = primitiveIngestionSchema.map(_.copy(array = Some(true)))

      assertValidateFun(
        overridedTypesForInvalidTests,
        List(inputRow),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) > 0).count() shouldBe 1
        val errors: Array[String] =
          validatedDF.select(explode(col(SL_ERROR_COL))).as[String].collect()

        errors should contain theSameElementsAs (List(
          "`timestamp[0]` (iso_date_time) doesn't match pattern `ISO_DATE_TIME`: 2011-12-03 10:15:30+01:00",
          "`date[0]` (date) doesn't match pattern `yyyy-MM-dd`: 2023-13-27",
          "`long[0]` (long) doesn't match pattern `[-+]?[0-9]+`: a10",
          "`int[0]` cannot be converted to `long` with sl type `int`: 9223372036854775808",
          "`double[0]` (double) doesn't match pattern `[-+]?\\d*\\.?\\d+[Ee]?[-+]?\\d*`: 12,5",
          "`decimal[0]` (decimal) doesn't match pattern `[+-]?\\d*\\.?\\d+(?:[eE][+-]?\\d+)?`: 012.5F-5",
          "`byte[0]` (byte) doesn't match pattern `.`: more than one byte",
          "`short[0]` (short) doesn't match pattern `[-+]?[0-9]+`: +",
          "`string[0]` (string) doesn't match pattern `.*CONTAINS.*`: string value\nthis is not the expected keyword: MISSING in \n the pattern.",
          "`boolean[0]` (boolean) doesn't match pattern `(?i)true|yes|[y1]|t<-TF->(?i)false|no|[n0]|f`: vrai"
        ))
      }
    }
  }

  it should "validate all primitive types with string type within a struct" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      implicit val spark: SparkSession = sparkSession

      val (primitiveInputRow, primitiveInputSchema, primitiveIngestionSchema) =
        primitiveStringType()
      val inputRow = Row(primitiveInputRow)
      val inputSchema = StructType(List(StructField("struct_root", primitiveInputSchema, true)))
      val ingestionSchema =
        List(Attribute(name = "struct_root", attributes = primitiveIngestionSchema))

      assertValidateFun(
        defaultTypes,
        List(inputRow, nullifyRowLeaves(inputRow)),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) === 0).count() shouldBe 2
      }
    }
  }

  it should "reject when pattern is invalid or overflow within a struct_root" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      implicit val spark: SparkSession = sparkSession
      import spark.implicits._

      val (primitiveInputRow, primitiveInputSchema, primitiveIngestionSchema) =
        invalidPrimitiveStringType()
      val inputRow = Row(primitiveInputRow)
      val inputSchema = StructType(List(StructField("struct_root", primitiveInputSchema, true)))
      val ingestionSchema =
        List(Attribute(name = "struct_root", attributes = primitiveIngestionSchema))

      assertValidateFun(
        overridedTypesForInvalidTests,
        List(inputRow),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) > 0).count() shouldBe 1
        val errors: Array[String] =
          validatedDF.select(explode(col(SL_ERROR_COL))).as[String].collect()

        errors should contain theSameElementsAs List(
          "`struct_root.timestamp` (iso_date_time) doesn't match pattern `ISO_DATE_TIME`: 2011-12-03 10:15:30+01:00",
          "`struct_root.date` (date) doesn't match pattern `yyyy-MM-dd`: 2023-13-27",
          "`struct_root.long` (long) doesn't match pattern `[-+]?[0-9]+`: a10",
          "`struct_root.int` cannot be converted to `long` with sl type `int`: 9223372036854775808",
          "`struct_root.double` (double) doesn't match pattern `[-+]?\\d*\\.?\\d+[Ee]?[-+]?\\d*`: 12,5",
          "`struct_root.decimal` (decimal) doesn't match pattern `[+-]?\\d*\\.?\\d+(?:[eE][+-]?\\d+)?`: 012.5F-5",
          "`struct_root.byte` (byte) doesn't match pattern `.`: more than one byte",
          "`struct_root.short` (short) doesn't match pattern `[-+]?[0-9]+`: +",
          "`struct_root.string` (string) doesn't match pattern `.*CONTAINS.*`: string value\nthis is not the expected keyword: MISSING in \n the pattern.",
          "`struct_root.boolean` (boolean) doesn't match pattern `(?i)true|yes|[y1]|t<-TF->(?i)false|no|[n0]|f`: vrai"
        )
      }
    }
  }

  it should "validate all primitive types with string type within an array of struct" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      implicit val spark: SparkSession = sparkSession

      val (primitiveInputRow, primitiveInputSchema, primitiveIngestionSchema) =
        primitiveStringType()
      val inputRow = Row(List(primitiveInputRow))
      val inputSchema =
        StructType(List(StructField("array_root", ArrayType(primitiveInputSchema), true)))
      val ingestionSchema =
        List(
          Attribute(name = "array_root", attributes = primitiveIngestionSchema, array = Some(true))
        )

      assertValidateFun(
        defaultTypes,
        List(inputRow, nullifyRowLeaves(inputRow)),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) === 0).count() shouldBe 2
      }
    }
  }

  it should "reject when pattern is invalid or overflow within an array of struct" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      implicit val spark: SparkSession = sparkSession
      import spark.implicits._

      val (primitiveInputRow, primitiveInputSchema, primitiveIngestionSchema) =
        invalidPrimitiveStringType()
      val inputRow = Row(List(primitiveInputRow))
      val inputSchema =
        StructType(List(StructField("array_root", ArrayType(primitiveInputSchema), true)))
      val ingestionSchema =
        List(
          Attribute(name = "array_root", attributes = primitiveIngestionSchema, array = Some(true))
        )

      assertValidateFun(
        overridedTypesForInvalidTests,
        List(inputRow, nullifyRowLeaves(inputRow)),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) > 0).count() shouldBe 1
        val errors: Array[String] =
          validatedDF.select(explode(col(SL_ERROR_COL))).as[String].collect()

        errors should contain theSameElementsAs List(
          "`array_root[0].timestamp` (iso_date_time) doesn't match pattern `ISO_DATE_TIME`: 2011-12-03 10:15:30+01:00",
          "`array_root[0].date` (date) doesn't match pattern `yyyy-MM-dd`: 2023-13-27",
          "`array_root[0].long` (long) doesn't match pattern `[-+]?[0-9]+`: a10",
          "`array_root[0].int` cannot be converted to `long` with sl type `int`: 9223372036854775808",
          "`array_root[0].double` (double) doesn't match pattern `[-+]?\\d*\\.?\\d+[Ee]?[-+]?\\d*`: 12,5",
          "`array_root[0].decimal` (decimal) doesn't match pattern `[+-]?\\d*\\.?\\d+(?:[eE][+-]?\\d+)?`: 012.5F-5",
          "`array_root[0].byte` (byte) doesn't match pattern `.`: more than one byte",
          "`array_root[0].short` (short) doesn't match pattern `[-+]?[0-9]+`: +",
          "`array_root[0].string` (string) doesn't match pattern `.*CONTAINS.*`: string value\nthis is not the expected keyword: MISSING in \n the pattern.",
          "`array_root[0].boolean` (boolean) doesn't match pattern `(?i)true|yes|[y1]|t<-TF->(?i)false|no|[n0]|f`: vrai"
        )
      }
    }
  }

  it should "reject on array or struct type mistmatch and missing required fields" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      implicit val spark: SparkSession = sparkSession
      import spark.implicits._

      val inputRow = Row(1, List("2"))
      val inputSchema =
        StructType(
          List(
            StructField("should_be_array", IntegerType, true),
            StructField("should_be_array_of_struct", ArrayType(StringType), true)
          )
        )
      val ingestionSchema =
        List(
          Attribute(
            name = "should_be_array",
            `type` = PrimitiveType.int.value,
            array = Some(true)
          ),
          Attribute(
            name = "should_be_array_of_struct",
            `type` = PrimitiveType.struct.value,
            attributes =
              List(Attribute(name = "struct_field", `type` = PrimitiveType.string.value)),
            array = Some(true)
          ),
          Attribute(
            name = "mandatory_field",
            `type` = PrimitiveType.string.value,
            required = Some(true)
          )
        )

      assertValidateFun(
        overridedTypesForInvalidTests,
        List(inputRow),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) > 0).count() shouldBe 1
        val errors: Array[String] =
          validatedDF.select(explode(col(SL_ERROR_COL))).as[String].collect()

        errors should contain theSameElementsAs List(
          "`should_be_array` is not an array: 1",
          "`should_be_array_of_struct` is not a struct: [2]",
          "`mandatory_field` is required: NULL"
        )
      }
    }
  }

  it should "reject with the correct indexes in array" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      implicit val spark: SparkSession = sparkSession
      import spark.implicits._

      val inputRow = Row(List("1", "a", "2", "b"))
      val inputSchema =
        StructType(List(StructField("int_array", ArrayType(StringType), true)))
      val ingestionSchema =
        List(
          Attribute(name = "int_array", `type` = PrimitiveType.int.value, array = Some(true))
        )

      assertValidateFun(
        overridedTypesForInvalidTests,
        List(inputRow, nullifyRowLeaves(inputRow)),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) > 0).count() shouldBe 1
        val errors: Array[String] =
          validatedDF.select(explode(col(SL_ERROR_COL))).as[String].collect()

        errors should contain theSameElementsAs List(
          "`int_array[1]` (int) doesn't match pattern `[-+]?[0-9]+`: a",
          "`int_array[3]` (int) doesn't match pattern `[-+]?[0-9]+`: b"
        )
      }
    }
  }

  "fitToSchema" should "convert string type to native type" in {
    new WithSettings() {
      implicit val schemaHandler = settings.schemaHandler()
      implicit val spark = sparkSession
      // ensure timezone doesn't makes unit test fail
      spark.conf.set("spark.sql.session.timeZone", "+06:00")
      // Create attributes for each primitive type

      val (inputRow, inputSchema, ingestionSchema) = primitiveStringType()
      assertSchemaFitFun(
        defaultTypes,
        List(inputRow, nullifyRowLeaves(inputRow)),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.schema should contain theSameElementsInOrderAs ingestionSchema.map { attr =>
          StructField(attr.name, attr.primitiveSparkType(schemaHandler), !attr.resolveRequired())
        }
        val nonEmptyRow = mapper.createObjectNode()
        nonEmptyRow
          .put("string", "string value")
          .put("variant", "{}")
          .put("long", 10)
          .put("int", 11)
          .put("short", 12)
          .put("double", 12.5)
          .put("decimal", 12.5)
          .put("boolean", true)
          .put("byte", 1)
          .put("date", "2023-10-27")
          .put("timestamp", "2011-12-03T11:15:30.000+06:00")

        val emptyRow = mapper.createObjectNode()
        validatedDF.toJSON.collect().map(mapper.readTree) should contain theSameElementsAs List(
          nonEmptyRow,
          emptyRow
        )
      }
    }
  }

  it should "keep all primitive types" in {
    // allows to support files such as parquet where timestamp or date are already defined
    new WithSettings() {
      implicit val schemaHandler = settings.schemaHandler()
      implicit val spark = sparkSession
      spark.conf.set("spark.sql.session.timeZone", "+06:00")

      val (inputRow, inputSchema, ingestionSchema) = primitiveNativeType()
      assertSchemaFitFun(
        defaultTypes,
        List(inputRow, nullifyRowLeaves(inputRow)),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.schema should contain theSameElementsInOrderAs ingestionSchema.map { attr =>
          StructField(attr.name, attr.primitiveSparkType(schemaHandler), !attr.resolveRequired())
        }
        val nonEmptyRow = mapper.createObjectNode()
        nonEmptyRow
          .put("string", "string value")
          .put("variant", "{}")
          .put("long", 10)
          .put("int", 11)
          .put("short", 12)
          .put("double", 12.5)
          .put("decimal", 12.5)
          .put("boolean", true)
          .put("byte", 1)
          .put("date", "2023-10-27")
          .put("timestamp", "2011-12-03T11:15:30.000+06:00")

        val emptyRow = mapper.createObjectNode()
        validatedDF.toJSON.collect().map(mapper.readTree) should contain theSameElementsAs List(
          nonEmptyRow,
          emptyRow
        )
      }
    }
  }

  it should "create missing columns with default value if defined" in {
    // allows to support files such as parquet where timestamp or date are already defined
    new WithSettings() {
      implicit val schemaHandler = settings.schemaHandler()
      implicit val spark = sparkSession
      spark.conf.set("spark.sql.session.timeZone", "+06:00")

      val inputRow = Row(Row(), 2.5)
      val inputSchema = StructType(
        List(StructField("struct", StructType(Nil), true), StructField("double", DoubleType, true))
      )
      val ingestionSchema = List(
        Attribute(name = "string", `type` = PrimitiveType.string.value, default = Some("It works")),
        Attribute(name = "int", `type` = PrimitiveType.int.value, default = Some("10")),
        Attribute(name = "long", `type` = PrimitiveType.int.value),
        Attribute(name = "double", `type` = PrimitiveType.double.value),
        Attribute(
          name = "struct",
          `type` = PrimitiveType.struct.value,
          attributes = List(
            Attribute(
              name = "nested_date",
              `type` = PrimitiveType.date.value,
              default = Some("2024-01-10")
            )
          )
        ),
        Attribute(
          name = "array",
          `type` = PrimitiveType.struct.value,
          attributes = List(
            Attribute(
              name = "nested_timestamp",
              `type` = PrimitiveType.timestamp.value,
              default = Some("2024-01-10T01:01:01")
            )
          ),
          array = Some(true)
        )
      )
      assertSchemaFitFun(
        defaultTypes,
        List(inputRow, nullifyRowLeaves(inputRow)),
        inputSchema,
        ingestionSchema
      ) { validatedDF =>
        makeFieldsNullable(validatedDF.schema) should contain theSameElementsInOrderAs Attribute(
          name = "__ROOT__",
          `type` = PrimitiveType.struct.value,
          attributes = ingestionSchema
        ).sparkType(schemaHandler).asInstanceOf[StructType].fields
        val nonEmptyRow = mapper.createObjectNode()
        nonEmptyRow
          .put("string", "It works")
          .put("int", 10)
          .put("double", 2.5)
          .putObject("struct")
          .put("nested_date", "2024-01-10")
        val emptyRow = mapper
          .createObjectNode()
        emptyRow
          .put("string", "It works")
          .put("int", 10)
          .putObject("struct")
          .put("nested_date", "2024-01-10")
        validatedDF.toJSON.collect().map(mapper.readTree) should contain theSameElementsAs List(
          nonEmptyRow,
          emptyRow
        )
      }
    }
  }

  "validate workflow" should "validate native input" in {
    new WithSettings() {
      implicit val schemaHandler = settings.schemaHandler()
      implicit val spark = sparkSession

      val (inputRow, inputSchema, ingestionSchema) = primitiveNativeType()
      val df = spark.createDataFrame(
        List(inputRow).asJava,
        inputSchema
      )
      val preparedDF =
        df.transform(
          new RowValidator(ingestionSchema, defaultTypes, Map.empty, true, true)
            .prepareData(Attributes.from(df.schema))
        )
      assertValidateFun(
        defaultTypes,
        preparedDF,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) === 0).count() shouldBe 1
      }
    }
  }

  it should "validate string input" in {
    new WithSettings() {
      implicit val schemaHandler = settings.schemaHandler()
      implicit val spark = sparkSession

      val (inputRow, inputSchema, ingestionSchema) = primitiveStringType()
      val df = spark.createDataFrame(
        List(inputRow).asJava,
        inputSchema
      )
      val preparedDF =
        df.transform(
          new RowValidator(ingestionSchema, defaultTypes, Map.empty, true, true)
            .prepareData(Attributes.from(df.schema))
        )
      assertValidateFun(
        defaultTypes,
        preparedDF,
        ingestionSchema
      ) { validatedDF =>
        validatedDF.filter(array_size(col(SL_ERROR_COL)) === 0).count() shouldBe 1
      }
    }
  }

  def makeFieldsNullable(structType: StructType): StructType = {
    structType.copy(fields = structType.fields.map {
      case sf @ StructField(_, ArrayType(elementType: StructType, _), _, _) =>
        sf.copy(dataType = ArrayType(makeFieldsNullable(elementType)), nullable = true)
      case sf @ StructField(_, ArrayType(elementType, _), _, _) =>
        sf.copy(dataType = ArrayType(elementType), nullable = true)
      case sf =>
        val targetType = sf.dataType match {
          case st: StructType => makeFieldsNullable(st)
          case _              => sf.dataType
        }
        sf.copy(dataType = targetType, nullable = true)
    })
  }
}
