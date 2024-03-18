package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.config.Settings.JdbcEngine.TableDdl
import ai.starlake.config.Settings.{
  latestSchemaVersion,
  AccessPolicies,
  AppConfig,
  Area,
  Audit,
  Connection,
  DagRef,
  ExpectationsConfig,
  Http,
  Internal,
  JdbcEngine,
  KafkaConfig,
  KafkaTopicConfig,
  Lock,
  Metrics,
  Privacy,
  SparkScheduling
}
import ai.starlake.config.{ApplicationDesc, Settings}
import ai.starlake.extract._
import ai.starlake.job.load.{IngestionNameStrategy, IngestionTimeStrategy}
import ai.starlake.schema.model._
import ai.starlake.utils.{Utils, YamlSerde}
import com.fasterxml.jackson.annotation.JsonInclude
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, DataTypes, StructField}
import org.apache.spark.storage.StorageLevel
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.util.TimeZone
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class YamlSerdeSpec extends TestHelper with ScalaCheckPropertyChecks {
  new WithSettings() {
    "Job with no explicit engine toMap" should "should produce the correct map" in {
      val task = AutoTaskDesc(
        name = "",
        sql = Some("select firstname, lastname, age from {{view}} where age=${age}"),
        database = None,
        domain = "user",
        table = "user",
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite)
      )
      val job =
        AutoJobDesc("user", List(task))
      val jobMap = YamlSerde.toMap(job)
      val expected = Map(
        "name" -> "user",
        "tasks" -> List(
          Map(
            "sql"           -> "select firstname, lastname, age from {{view}} where age=${age}",
            "domain"        -> "user",
            "table"         -> "user",
            "writeStrategy" -> Map("type" -> "OVERWRITE")
          )
        )
      )
      assert((jobMap.toSet diff expected.toSet).toMap.isEmpty)
    }
    "Job wit BQ engine toMap" should "should produce the correct map with right engine" in {
      val task = AutoTaskDesc(
        name = "",
        sql = Some("select firstname, lastname, age from dataset.table where age=${age}"),
        None,
        domain = "user",
        table = "user",
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite)
      )
      val job =
        AutoJobDesc(
          "user",
          List(task)
        )
      val jobMap = YamlSerde.toMap(job)
      val expected: Map[String, Any] = Map(
        "name" -> "user",
        "tasks" -> List(
          Map(
            "sql"    -> "select firstname, lastname, age from dataset.table where age=${age}",
            "domain" -> "user",
            "table"  -> "user",
            "writeStrategy" -> Map("type" -> "OVERWRITE")
          )
        )
      )
      assert((expected.toSet diff jobMap.toSet).toMap.isEmpty)
    }
    "Job with SPARK engine toMap" should "should produce the correct map" in {
      val task = AutoTaskDesc(
        name = "",
        sql = Some("select firstname, lastname, age from {{view}} where age=${age}"),
        database = None,
        domain = "user",
        table = "user",
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite)
      )
      val job =
        AutoJobDesc("user", List(task))
      val jobMap = YamlSerde.toMap(job)
      val expected: Map[String, Any] = Map(
        "name" -> "user",
        "tasks" -> List(
          Map(
            "sql"           -> "select firstname, lastname, age from {{view}} where age=${age}",
            "table"         -> "user",
            "domain"        -> "user",
            "writeStrategy" -> Map("type" -> "OVERWRITE")
          )
        )
      )
      println(jobMap)
      println(expected)
      assert((expected.toSet diff jobMap.toSet).toMap.isEmpty)
    }
  }

  "YamlSerializer" should "round-trip any Yaml Extract Config" in {
    import YamlConfigGenerators._
    forAll { (yamlExtractConfig: ExtractDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlExtractConfig)
      try {
        val deserializedConfig = YamlSerde.deserializeYamlExtractConfig(config, "input")
        deserializedConfig should equal(yamlExtractConfig.extract.propageGlobalJdbcSchemas())
      } catch {
        case e: Exception =>
          logger.info("Generated config\n" + config)
          throw e
      }
    }
  }

  it should "round-trip any Yaml Dag Config" in {
    import YamlConfigGenerators._
    forAll { (yamlDagConfig: DagDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlDagConfig)
      YamlSerde
        .deserializeYamlDagConfig(config, "input")
        .map(_ should equal(yamlDagConfig.dag)) match {
        case Failure(exception) =>
          logger.info("Generated config\n" + config)
          throw exception
        case Success(_) =>
        // assertions done
      }
    }
  }

  it should "round-trip any Yaml Load Config" in {
    import YamlConfigGenerators._
    forAll { (yamlLoadConfig: LoadDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlLoadConfig)
      YamlSerde
        .deserializeYamlLoadConfig(config, "input")
        .map { deserializedConfig =>
          deserializedConfig.tables
            .map(_.pattern)
            .zip(yamlLoadConfig.load.tables.map(_.pattern))
            .foreach { case (p1, p2) =>
              p1.toString should equal(p2.toString)
            }
          val substitutePattern = Pattern.compile("dummy")
          deserializedConfig.tables.map(_.copy(pattern = substitutePattern)) should equal(
            yamlLoadConfig.load.tables.map(_.copy(pattern = substitutePattern))
          )
        } match {
        case Failure(exception) =>
          logger.info("Generated config\n" + config)
          throw exception
        case Success(_) =>
        // assertions done
      }
    }
  }

  it should "round-trip any Yaml Refs Config" in {
    import YamlConfigGenerators._
    forAll { (yamlRefConfig: RefDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlRefConfig)
      try {
        val deserializedConfig = YamlSerde.deserializeYamlRefs(config, "input")
        val substitutedInputRef = InputRef(
          Pattern.compile("a"),
          None,
          None
        )
        deserializedConfig.refs.map(_.input).zip(yamlRefConfig.refs.map(_.input)).foreach {
          case (iDes, iOrig) =>
            iDes.domain.map(_.toString) should equal(iOrig.domain.map(_.toString))
            iDes.database.map(_.toString) should equal(iOrig.database.map(_.toString))
            iDes.table.toString should equal(iOrig.table.toString)
        }
        deserializedConfig.refs.map(_.copy(input = substitutedInputRef)) should equal(
          yamlRefConfig.refs.map(_.copy(input = substitutedInputRef))
        )
      } catch {
        case e: Exception =>
          logger.info("Generated config\n" + config)
          throw e
      }
    }
  }

  it should "round-trip any Yaml Application Config" in {
    import YamlConfigGenerators._
    forAll { (yamlApplicationConfig: ApplicationDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlApplicationConfig)
      try {
        val deserializedConfig = YamlSerde.deserializeYamlApplication(config, "input")
        mapperWithEmptyString.writeValueAsString(deserializedConfig) should equal(config)
      } catch {
        case e: Exception =>
          logger.info("Generated config\n" + config)
          throw e
      }
    }
  }

  it should "round-trip any Yaml External Config" in {
    import YamlConfigGenerators._
    forAll { (yamlExternalConfig: ExternalDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlExternalConfig)
      try {
        val deserializedConfig = YamlSerde.deserializeYamlExternal(config, "input")
        deserializedConfig should equal(yamlExternalConfig.external.projects.getOrElse(Nil))
      } catch {
        case e: Exception =>
          logger.info("Generated config\n" + config)
          throw e
      }
    }
  }

  it should "round-trip any Yaml Task Config" in {
    import YamlConfigGenerators._
    forAll { (yamlTaskConfig: TaskDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlTaskConfig)
      try {
        val deserializedConfig = YamlSerde.deserializeYamlTask(config, "input")
        mapperWithEmptyString.writeValueAsString(deserializedConfig) should equal(
          mapperWithEmptyString.writeValueAsString(yamlTaskConfig.task)
        )
      } catch {
        case e: Exception =>
          logger.info("Generated config\n" + config)
          throw e
      }
    }
  }

  it should "round-trip any Yaml Transform Config" in {
    import YamlConfigGenerators._
    forAll { (yamlTransformConfig: TransformDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlTransformConfig)
      YamlSerde.deserializeYamlTransform(config, "input").map { job =>
        job should equal(yamlTransformConfig.transform)
      } match {
        case Failure(exception) =>
          logger.info("Generated config\n" + config)
          throw exception
        case Success(_) =>
        // assertions done
      }
    }
  }

  it should "round-trip any Yaml Table Config" in {
    import YamlConfigGenerators._
    forAll { (yamlTableConfig: TableDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlTableConfig)
      try {
        val deserializedConfig = YamlSerde.deserializeYamlTables(config, "input")
        deserializedConfig.tables.size shouldBe 1
        deserializedConfig.tables.head.pattern.toString should equal(
          yamlTableConfig.table.pattern.toString
        )
        val substitutedPattern = Pattern.compile("hello")
        deserializedConfig.tables.map(_.copy(pattern = substitutedPattern)) should equal(
          List(yamlTableConfig.table.copy(pattern = substitutedPattern))
        )
      } catch {
        case e: Exception =>
          logger.info("Generated config\n" + config)
          throw e
      }
    }
  }

  it should "round-trip any Yaml Tables Config" in {
    import YamlConfigGenerators._
    forAll { (yamlTableConfig: TablesDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlTableConfig)
      try {
        val deserializedConfig = YamlSerde.deserializeYamlTables(config, "input")
        deserializedConfig.tables
          .map(_.pattern)
          .zip(yamlTableConfig.tables.map(_.pattern))
          .foreach { case (p1, p2) =>
            p1.toString should equal(p2.toString)
          }
        val substitutedPattern = Pattern.compile("hello")
        deserializedConfig.tables.map(_.copy(pattern = substitutedPattern)) should equal(
          yamlTableConfig.tables.map(_.copy(pattern = substitutedPattern))
        )
      } catch {
        case e: Exception =>
          logger.info("Generated config\n" + config)
          throw e
      }
    }
  }

  it should "round-trip any Yaml Types Config" in {
    import YamlConfigGenerators._
    forAll { (yamlTypesConfig: TypesDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlTypesConfig)
      try {
        val deserializedConfig = YamlSerde.deserializeYamlTypes(config, "input")
        deserializedConfig should equal(yamlTypesConfig.types)
      } catch {
        case e: Exception =>
          logger.info("Generated config\n" + config)
          throw e
      }
    }
  }

  it should "round-trip any Yaml Env Config" in {
    import YamlConfigGenerators._
    forAll { (yamlTypesConfig: EnvDesc) =>
      val mapperWithEmptyString =
        Utils.newYamlMapper().setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      val config = mapperWithEmptyString.writeValueAsString(yamlTypesConfig)
      try {
        val deserializedConfig = YamlSerde.deserializeYamlEnvConfig(config, "input")
        deserializedConfig should equal(yamlTypesConfig)
      } catch {
        case e: Exception =>
          logger.info("Generated config\n" + config)
          throw e
      }
    }
  }
}

object YamlConfigGenerators {
  val maxElementInCollections = 5

  implicit val arbAsciiString: Arbitrary[String] = Arbitrary(
    Gen.containerOf[Array, Char](Gen.choose[Char](32, 127)).map(_.mkString.trim)
  )

  implicit def restrictedMapAnyAny[T, U](implicit a: Arbitrary[(T, U)]): Arbitrary[Map[T, U]] =
    Arbitrary {
      Gen.mapOf[T, U](a.arbitrary).map(_.take(maxElementInCollections))
    }

  implicit def restrictedListAny[T](implicit a: Arbitrary[T]): Arbitrary[List[T]] = Arbitrary {
    Gen.containerOf[List, T](a.arbitrary).map(_.take(maxElementInCollections))
  }

  implicit val pattern: Arbitrary[Pattern] = Arbitrary {
    Gen.oneOf(
      Pattern.compile("azeqsd"),
      Pattern.compile(".*"),
      Pattern.compile(".*.*"),
      Pattern.compile("(one|two)")
    )
  }

  implicit val tableColumn: Arbitrary[TableColumn] = Arbitrary {
    for {
      name   <- arbitrary[String].filter(_.nonEmpty)
      rename <- Gen.option(arbitrary[String])
    } yield TableColumn(name = name, rename = rename)
  }
  implicit val jdbcTable: Arbitrary[JDBCTable] = Arbitrary {
    for {
      name              <- arbitrary[String].filter(_.nonEmpty)
      tableColumns      <- arbitrary[List[TableColumn]].filter(_.nonEmpty)
      partitionColumn   <- Gen.option(arbitrary[String])
      numPartitions     <- Gen.option(arbitrary[Int])
      connectionOptions <- arbitrary[Map[String, String]]
      fetchSize         <- Gen.option(arbitrary[Int])
      fullExport        <- Gen.option(arbitrary[Boolean])
    } yield JDBCTable(
      name = name,
      columns = tableColumns,
      partitionColumn = partitionColumn,
      numPartitions = numPartitions,
      connectionOptions = connectionOptions,
      fetchSize = fetchSize,
      fullExport = fullExport
    )
  }

  implicit val trim: Arbitrary[Trim] = Arbitrary {
    Gen.oneOf(Trim.modes)
  }

  implicit val jdbcSchema: Arbitrary[JDBCSchema] = Arbitrary {
    for {
      catalog             <- Gen.option(arbitrary[String])
      schema              <- arbitrary[String]
      tableRemarks        <- Gen.option(arbitrary[String])
      columnRemarks       <- Gen.option(arbitrary[String])
      tables              <- arbitrary[List[JDBCTable]]
      exclude             <- arbitrary[List[String]]
      tableTypes          <- arbitrary[List[String]]
      template            <- Gen.option(arbitrary[String])
      pattern             <- Gen.option(arbitrary[Pattern].map(_.toString))
      numericTrim         <- Gen.option(arbitrary[Trim])
      partitionColumn     <- Gen.option(arbitrary[String])
      numPartitions       <- Gen.option(arbitrary[Int])
      connectionOptions   <- arbitrary[Map[String, String]]
      fetchSize           <- Gen.option(arbitrary[Int])
      stringPartitionFunc <- Gen.option(arbitrary[String])
      fullExport          <- Gen.option(arbitrary[Boolean])
      sanitizeName        <- Gen.option(arbitrary[Boolean])
    } yield JDBCSchema(
      catalog = catalog,
      schema = schema,
      tableRemarks = tableRemarks,
      columnRemarks = columnRemarks,
      tables = tables,
      exclude = exclude,
      tableTypes = tableTypes,
      template = template,
      pattern = pattern,
      numericTrim = numericTrim,
      partitionColumn = partitionColumn,
      numPartitions = numPartitions,
      connectionOptions = connectionOptions,
      fetchSize = fetchSize,
      stringPartitionFunc = stringPartitionFunc,
      fullExport = fullExport,
      sanitizeName = sanitizeName
    )
  }

  implicit val fileFormat: Arbitrary[FileFormat] = Arbitrary {
    for {
      encoding         <- Gen.option(arbitrary[String])
      withHeader       <- Gen.option(arbitrary[Boolean])
      separator        <- Gen.option(arbitrary[String])
      quote            <- Gen.option(arbitrary[String])
      escape           <- Gen.option(arbitrary[String])
      nullValue        <- Gen.option(arbitrary[String])
      datePattern      <- Gen.option(arbitrary[String])
      timestampPattern <- Gen.option(arbitrary[String])
    } yield FileFormat(
      encoding = encoding,
      withHeader = withHeader,
      separator = separator,
      quote = quote,
      escape = escape,
      nullValue = nullValue,
      datePattern = datePattern,
      timestampPattern = timestampPattern
    )
  }

  implicit val jdbcSchemas: Arbitrary[JDBCSchemas] = Arbitrary {
    for {
      jdbcSchemas <- arbitrary[List[JDBCSchema]]
      default <- Gen.option(
        arbitrary[JDBCSchema].map(_.copy(tables = Nil, exclude = Nil))
      )
      output             <- Gen.option(arbitrary[FileFormat])
      connectionRef      <- Gen.option(arbitrary[String])
      auditConnectionRef <- Gen.option(arbitrary[String])
    } yield JDBCSchemas(
      jdbcSchemas = jdbcSchemas,
      default = default,
      output = output,
      connectionRef = connectionRef,
      auditConnectionRef = auditConnectionRef
    )
  }

  implicit val extractDesc: Arbitrary[ExtractDesc] = Arbitrary {
    for {
      jdbcSchemas <- arbitrary[JDBCSchemas]
    } yield ExtractDesc(latestSchemaVersion, extract = jdbcSchemas)
  }

  implicit val dagGenerationConfig: Arbitrary[DagGenerationConfig] = Arbitrary {
    for {
      comment  <- arbitrary[String].filter(_.nonEmpty)
      template <- arbitrary[String].filter(_.nonEmpty)
      filename <- arbitrary[String].filter(_.nonEmpty)
      options  <- arbitrary[Map[String, String]]
    } yield DagGenerationConfig(
      comment = comment,
      template = template,
      filename = filename,
      options = options
    )
  }

  implicit val dagDesc: Arbitrary[DagDesc] = Arbitrary {
    for {
      dagGenerationConfig <- arbitrary[DagGenerationConfig]
    } yield DagDesc(latestSchemaVersion, dagGenerationConfig)
  }

  implicit val format: Arbitrary[Format] = Arbitrary {
    Gen.oneOf(Format.formats)
  }

  implicit val writeMode: Arbitrary[WriteMode] = Arbitrary {
    Gen.oneOf(WriteMode.writes)
  }

  implicit val allSinks: Arbitrary[AllSinks] = Arbitrary {
    for {
      connectionRef          <- Gen.option(arbitrary[String])
      clustering             <- Gen.option(arbitrary[List[String]])
      days                   <- Gen.option(arbitrary[Int])
      requirePartitionFilter <- Gen.option(arbitrary[Boolean])
      materializedView       <- Gen.option(arbitrary[Boolean])
      enableRefresh          <- Gen.option(arbitrary[Boolean])
      refreshIntervalMs      <- Gen.option(arbitrary[Long])
      id                     <- Gen.option(arbitrary[String])
      // timestamp: Option[String] = None,
      // options: Option[Map[String, String]] = None,

      // FS
      format    <- Gen.option(arbitrary[String])
      extension <- Gen.option(arbitrary[String])

      // clustering: Option[Seq[String]] = None,
      partition <- Gen.option(arbitrary[List[String]])
      coalesce  <- Gen.option(arbitrary[Boolean])
      options   <- Gen.option(arbitrary[Map[String, String]])
    } yield AllSinks(
      connectionRef = connectionRef,
      clustering = clustering,
      days = days,
      requirePartitionFilter = requirePartitionFilter,
      materializedView = materializedView,
      enableRefresh = enableRefresh,
      refreshIntervalMs = refreshIntervalMs,
      id = id,
      format = format,
      extension = extension,
      partition = partition,
      coalesce = coalesce,
      options = options
    )
  }

  implicit val freshness: Arbitrary[Freshness] = Arbitrary {
    for {
      warn  <- Gen.option(arbitrary[String])
      error <- Gen.option(arbitrary[String])
    } yield Freshness(warn, error)
  }

  implicit val writeStrategyType: Arbitrary[WriteStrategyType] = Arbitrary {
    Gen.oneOf(WriteStrategyType.strategies)
  }

  implicit val writeStrategy: Arbitrary[WriteStrategy] = Arbitrary {
    for {
      strategyType <- Gen.option(arbitrary[WriteStrategyType])
      types        <- Gen.option(arbitrary[Map[String, String]])
      key          <- arbitrary[List[String]]
      timestamp    <- Gen.option(arbitrary[String])
      queryFilter  <- Gen.option(arbitrary[String])
      on           <- Gen.option(arbitrary[MergeOn])
      startTs      <- Gen.option(arbitrary[String])
      endTs        <- Gen.option(arbitrary[String])
    } yield {
      WriteStrategy(
        `type` = strategyType,
        types = types,
        key = key,
        timestamp = timestamp,
        queryFilter = queryFilter,
        on = on,
        startTs = startTs,
        endTs = endTs
      )
    }
  }

  implicit val metadata: Arbitrary[Metadata] = Arbitrary {
    for {
      format        <- Gen.option(arbitrary[Format])
      encoding      <- Gen.option(arbitrary[String])
      multiline     <- Gen.option(arbitrary[Boolean])
      array         <- Gen.option(arbitrary[Boolean])
      withHeader    <- Gen.option(arbitrary[Boolean])
      separator     <- Gen.option(arbitrary[String])
      quote         <- Gen.option(arbitrary[String])
      escape        <- Gen.option(arbitrary[String])
      writeStrategy <- Gen.option(arbitrary[WriteStrategy])
      sink          <- Gen.option(arbitrary[AllSinks])
      ignore        <- Gen.option(arbitrary[String])
      directory     <- Gen.option(arbitrary[String])
      ack           <- Gen.option(arbitrary[String])
      options       <- Gen.option(arbitrary[Map[String, String]])
      loader        <- Gen.option(arbitrary[String])
      emptyIsNull   <- Gen.option(arbitrary[Boolean])
      dagRef        <- Gen.option(arbitrary[String])
      freshness     <- Gen.option(arbitrary[Freshness])
      nullValue     <- Gen.option(arbitrary[String])
      schedule      <- Gen.option(arbitrary[String])
    } yield {
      val metadataInstance = Metadata(
        format = format,
        encoding = encoding,
        multiline = multiline,
        array = array,
        withHeader = withHeader,
        separator = separator,
        quote = quote,
        escape = escape,
        sink = sink,
        ignore = ignore,
        directory = directory,
        ack = ack,
        options = options,
        loader = loader,
        emptyIsNull = emptyIsNull,
        dagRef = dagRef,
        freshness = freshness,
        nullValue = nullValue,
        schedule = schedule,
        writeStrategy = writeStrategy
      )
      // Instantiate with serialized default value otherwise comparison in round-trip would fail
      metadataInstance
        .copy(
          format = Some(metadataInstance.getFormat()),
          encoding = Some(metadataInstance.getEncoding()),
          multiline = Some(metadataInstance.getMultiline()),
          array = Some(metadataInstance.isArray()),
          withHeader = Some(metadataInstance.isWithHeader()),
          separator = Some(metadataInstance.getSeparator()),
          quote = Some(metadataInstance.getQuote()),
          escape = Some(metadataInstance.getEscape()),
          options = Some(metadataInstance.getOptions()),
          nullValue = Option(
            metadataInstance.getNullValue()
          ),
          emptyIsNull = Some(metadataInstance.isEmptyIsNull())
        )
    }
  }

  implicit val privacyLevel: Arbitrary[TransformInput] = Arbitrary {
    for {
      value <- arbitrary[String]
      sql   <- arbitrary[Boolean]
    } yield TransformInput(value, sql)
  }

  implicit val metricType: Arbitrary[MetricType] = Arbitrary {
    Gen.oneOf(MetricType.metricTypes)
  }

  implicit val position: Arbitrary[Position] = Arbitrary {
    for {
      first <- arbitrary[Int]
      last  <- arbitrary[Int]
    } yield Position(first, last)
  }

  implicit val dataType: Arbitrary[DataType] = Arbitrary {
    val primitiveTypes = List(
      DataTypes.StringType,
      DataTypes.LongType,
      DataTypes.IntegerType,
      DataTypes.DateType,
      DataTypes.BooleanType,
      DataTypes.DoubleType,
      DataTypes.FloatType,
      DataTypes.ShortType,
      DataTypes.TimestampType
    )
    val arrayTypeGen: Gen[DataType] = Gen.oneOf(primitiveTypes).map(DataTypes.createArrayType)
    val primitiveTypesGen: List[Gen[DataType]] = primitiveTypes.map(dt => Gen.oneOf(List(dt)))
    Gen.oneOf(arrayTypeGen, primitiveTypesGen.head, primitiveTypesGen.tail: _*)
  }

  implicit val structField: Arbitrary[StructField] = Arbitrary {
    for {
      name     <- arbitrary[String]
      dataType <- arbitrary[DataType]
      nullable <- arbitrary[Boolean]
    } yield StructField(name, dataType, nullable)
  }

  implicit val attribute: Arbitrary[Attribute] = Arbitrary {
    arbitrary[StructField].map { sf =>
      val attributeInstance = Attribute(sf)
      // Instantiate with serialized default value otherwise comparison in round-trip would fail
      attributeInstance.copy(
        ignore = Some(attributeInstance.isIgnore()),
        array = Some(attributeInstance.isArray())
      )
    }
  }

  implicit val mergeOn: Arbitrary[MergeOn] = Arbitrary {
    Gen.oneOf(MergeOn.mergeOns)
  }

  implicit val rowLevelSecurity: Arbitrary[RowLevelSecurity] = Arbitrary {
    for {
      name        <- arbitrary[String]
      predicate   <- arbitrary[String]
      grants      <- arbitrary[List[String]].map(_.toSet)
      description <- arbitrary[String]
    } yield RowLevelSecurity(
      name = name,
      predicate = predicate,
      grants = grants,
      description = description
    )
  }

  implicit val expectationItem: Arbitrary[ExpectationItem] = Arbitrary {
    for {
      query  <- arbitrary[String].map(_.replace("=>", "_").replace(")", "_"))
      expect <- arbitrary[String]
    } yield {
      val ei = ExpectationItem(query = query, expect = expect)
      ei
    }
  }

  implicit val accessControlEntry: Arbitrary[AccessControlEntry] = Arbitrary {
    for {
      role   <- arbitrary[String]
      grants <- arbitrary[List[String]].map(_.toSet)
      name   <- arbitrary[String]
    } yield AccessControlEntry(name = name, grants = grants, role = role)
  }

  implicit val schema: Arbitrary[Schema] = Arbitrary {
    for {
      name          <- arbitrary[String]
      attributes    <- arbitrary[List[Attribute]]
      metadata      <- Gen.option(arbitrary[Metadata])
      comment       <- Gen.option(arbitrary[String])
      presql        <- arbitrary[List[String]]
      postsql       <- arbitrary[List[String]]
      tags          <- arbitrary[List[String]].map(_.toSet)
      rls           <- arbitrary[List[RowLevelSecurity]]
      expectations  <- arbitrary[List[ExpectationItem]]
      primaryKey    <- arbitrary[List[String]]
      acl           <- arbitrary[List[AccessControlEntry]]
      rename        <- Gen.option(arbitrary[String])
      sample        <- Gen.option(arbitrary[String])
      filter        <- Gen.option(arbitrary[String])
      patternSample <- Gen.option(arbitrary[String])
      pattern       <- arbitrary[Pattern]
    } yield Schema(
      name = name,
      attributes = attributes,
      metadata = metadata,
      comment = comment,
      presql = presql,
      postsql = postsql,
      tags = tags,
      rls = rls,
      expectations = expectations,
      primaryKey = primaryKey,
      acl = acl,
      rename = rename,
      sample = sample,
      filter = filter,
      patternSample = patternSample,
      pattern = pattern
    )
  }

  implicit val domain: Arbitrary[Domain] = Arbitrary {
    for {
      name     <- arbitrary[String]
      metadata <- Gen.option(arbitrary[Metadata])
      tables   <- arbitrary[List[Schema]]
      comment  <- Gen.option(arbitrary[String])
      tags     <- arbitrary[List[String]].map(_.toSet)
      rename   <- Gen.option(arbitrary[String])
      database <- Gen.option(arbitrary[String])
    } yield Domain(
      name = name,
      metadata = metadata,
      tables = tables,
      comment = comment,
      tags = tags,
      rename = rename,
      database = database
    )
  }

  implicit val loadDesc: Arbitrary[LoadDesc] = Arbitrary {
    for {
      domain <- arbitrary[Domain]
    } yield LoadDesc(latestSchemaVersion, load = domain)
  }

  implicit val inputRef: Arbitrary[InputRef] = Arbitrary {
    for {
      table    <- arbitrary[Pattern]
      domain   <- Gen.option(arbitrary[Pattern])
      database <- Gen.option(arbitrary[Pattern])
    } yield InputRef(table = table, domain = domain, database = database)
  }

  implicit val outputRef: Arbitrary[OutputRef] = Arbitrary {
    for {
      database <- arbitrary[String]
      domain   <- arbitrary[String]
      table    <- arbitrary[String]
    } yield OutputRef(database = database, domain = domain, table = table)
  }

  implicit val ref: Arbitrary[Ref] = Arbitrary {
    for {
      input  <- arbitrary[InputRef]
      output <- arbitrary[OutputRef]
    } yield Ref(input = input, output = output)
  }

  implicit val refDesc: Arbitrary[RefDesc] = Arbitrary {
    for {
      refs <- arbitrary[List[Ref]]
    } yield RefDesc(latestSchemaVersion, refs = refs)
  }

  implicit val audit: Arbitrary[Audit] = Arbitrary {
    for {
      path      <- arbitrary[String]
      sink      <- arbitrary[AllSinks]
      maxErrors <- arbitrary[Int]
      database  <- Gen.option(arbitrary[String])
      domain    <- Gen.option(arbitrary[String])
      active    <- Gen.option(arbitrary[Boolean])
      sql       <- Gen.option(arbitrary[String])
    } yield Audit(
      path = path,
      sink = sink,
      maxErrors = maxErrors,
      database = database,
      domain = domain,
      active = active,
      sql = sql
    )
  }

  implicit val metrics: Arbitrary[Metrics] = Arbitrary {
    for {
      path                   <- arbitrary[String]
      discreteMaxCardinality <- arbitrary[Int]
      active                 <- arbitrary[Boolean]
    } yield Metrics(path = path, discreteMaxCardinality = discreteMaxCardinality, active = active)
  }

  implicit val lock: Arbitrary[Lock] = Arbitrary {
    for {
      path        <- arbitrary[String]
      timeout     <- arbitrary[Long]
      pollTime    <- arbitrary[FiniteDuration]
      refreshTime <- arbitrary[FiniteDuration]
    } yield Lock(path = path, timeout = timeout, pollTime = pollTime, refreshTime = refreshTime)
  }

  implicit val area: Arbitrary[Area] = Arbitrary {
    for {
      stage        <- arbitrary[String]
      unresolved   <- arbitrary[String]
      archive      <- arbitrary[String]
      ingesting    <- arbitrary[String]
      replay       <- arbitrary[String]
      hiveDatabase <- arbitrary[String]
    } yield Area(
      stage = stage,
      unresolved = unresolved,
      archive = archive,
      ingesting = ingesting,
      replay = replay,
      hiveDatabase = hiveDatabase
    )
  }

  implicit val connection: Arbitrary[Connection] = Arbitrary {
    for {
      sparkFormat <- Gen.option(arbitrary[String])
      connectionType <- Gen.oneOf(
        "GCPLOG",
        "LOCAL",
        "FS",
        "FILESYSTEM",
        "HIVE",
        "DATABRICKS",
        "SPARK",
        "JDBC",
        "BIGQUERY",
        "BQ",
        "ES",
        "ELASTICSEARCH",
        "KAFKA"
      )
      quote     <- Gen.option(arbitrary[String])
      separator <- Gen.option(arbitrary[String])
      options <- arbitrary[Map[String, String]].map(m =>
        if (connectionType.contains("JDBC"))
          m + ("url" -> "jdbc:mysql://myhost")
        else m
      )
    } yield Connection(
      `type` = connectionType,
      sparkFormat = sparkFormat,
      quote = quote,
      separator = separator,
      options = options
    )
  }

  implicit val tableDdl: Arbitrary[TableDdl] = Arbitrary {
    for {
      createSql <- arbitrary[String]
      pingSql   <- Gen.option(arbitrary[String])
      selectSql <- Gen.option(arbitrary[String])
    } yield TableDdl(createSql = createSql, pingSql = pingSql, selectSql = selectSql)
  }

  implicit val jdbcEngine: Arbitrary[JdbcEngine] = Arbitrary {
    for {
      tables          <- arbitrary[Map[String, TableDdl]]
      canMerge        <- arbitrary[Boolean]
      viewPrefix      <- arbitrary[String]
      quote           <- arbitrary[String]
      preActions      <- arbitrary[String]
      strategyBuilder <- arbitrary[String]
    } yield JdbcEngine(
      tables = tables,
      canMerge = canMerge,
      quote = quote,
      viewPrefix = viewPrefix,
      preActions = preActions,
      strategyBuilder = strategyBuilder
    )
  }

  implicit val privacy: Arbitrary[Privacy] = Arbitrary {
    for {
      options <- arbitrary[Map[String, String]]
    } yield Privacy(options)
  }

  implicit val storageLevel: Arbitrary[StorageLevel] = Arbitrary {
    Gen.oneOf(
      StorageLevel.NONE,
      StorageLevel.OFF_HEAP,
      StorageLevel.DISK_ONLY,
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_ONLY
    )
  }

  implicit val internal: Arbitrary[Internal] = Arbitrary {
    for {
      cacheStorageLevel          <- arbitrary[StorageLevel]
      intermediateBigqueryFormat <- arbitrary[String]
      temporaryGcsBucket         <- Gen.option(arbitrary[String])
      substituteVars             <- arbitrary[Boolean]
      bqAuditSaveInBatchMode     <- arbitrary[Boolean]
    } yield Internal(
      cacheStorageLevel,
      intermediateBigqueryFormat = intermediateBigqueryFormat,
      temporaryGcsBucket = temporaryGcsBucket,
      substituteVars = substituteVars,
      bqAuditSaveInBatchMode = bqAuditSaveInBatchMode
    )
  }

  implicit val accessPolicies: Arbitrary[AccessPolicies] = Arbitrary {
    for {
      apply    <- arbitrary[Boolean]
      location <- arbitrary[String]
      database <- arbitrary[String]
      taxonomy <- arbitrary[String]
    } yield AccessPolicies(
      apply = apply,
      location = location,
      database = database,
      taxonomy = taxonomy
    )
  }

  implicit val sparkScheduling: Arbitrary[SparkScheduling] = Arbitrary {
    for {
      maxJobs  <- arbitrary[Int]
      poolName <- arbitrary[String]
      mode     <- arbitrary[String]
      file     <- arbitrary[String]
    } yield SparkScheduling(maxJobs = maxJobs, poolName = poolName, mode = mode, file = file)
  }

  implicit val expectations: Arbitrary[ExpectationsConfig] = Arbitrary {
    for {
      path        <- arbitrary[String]
      active      <- arbitrary[Boolean]
      failOnError <- arbitrary[Boolean]
    } yield ExpectationsConfig(path, active, failOnError)
  }

  implicit val kafkaTopicConfig: Arbitrary[KafkaTopicConfig] = Arbitrary {
    for {
      topicName         <- arbitrary[String]
      maxRead           <- arbitrary[Long]
      fields            <- arbitrary[List[String]]
      partitions        <- arbitrary[Int]
      replicationFactor <- arbitrary[Short]
      createOptions     <- arbitrary[Map[String, String]]
      accessOptions     <- arbitrary[Map[String, String]]
      headers           <- arbitrary[Map[String, Map[String, String]]]
    } yield KafkaTopicConfig(
      topicName = topicName,
      maxRead = maxRead,
      fields = fields,
      partitions = partitions,
      replicationFactor = replicationFactor,
      createOptions = createOptions,
      accessOptions = accessOptions,
      headers = headers
    )
  }

  implicit val kafkaConfig: Arbitrary[KafkaConfig] = Arbitrary {
    for {
      serverOptions    <- arbitrary[Map[String, String]]
      topics           <- Gen.mapOf[String, KafkaTopicConfig](arbitrary[(String, KafkaTopicConfig)])
      cometOffsetsMode <- Gen.option(arbitrary[String])
      customDeserializers <- Gen.option(arbitrary[Map[String, String]])
    } yield KafkaConfig(
      serverOptions = serverOptions,
      topics = topics,
      cometOffsetsMode = cometOffsetsMode,
      customDeserializers = customDeserializers
    )
  }

  implicit val dagRef: Arbitrary[DagRef] = Arbitrary {
    for {
      load      <- Gen.option(arbitrary[String])
      transform <- Gen.option(arbitrary[String])
    } yield DagRef(load = load, transform = transform)
  }

  implicit val http: Arbitrary[Http] = Arbitrary {
    for {
      interface <- arbitrary[String]
      port      <- arbitrary[Int]
    } yield Http(interface = interface, port = port)
  }

  implicit val timezone: Arbitrary[TimeZone] = Arbitrary {
    Gen.oneOf(TimeZone.getAvailableIDs).map(TimeZone.getTimeZone)
  }

  implicit val appConfig: Arbitrary[AppConfig] = Arbitrary {
    for {
      env                        <- arbitrary[String]
      datasets                   <- arbitrary[String]
      incoming                   <- arbitrary[String]
      dags                       <- arbitrary[String]
      writeStrategies            <- arbitrary[String]
      metadata                   <- arbitrary[String]
      metrics                    <- arbitrary[Metrics]
      validateOnLoad             <- arbitrary[Boolean]
      audit                      <- arbitrary[Audit]
      archive                    <- arbitrary[Boolean]
      sinkReplayToFile           <- arbitrary[Boolean]
      lock                       <- arbitrary[Lock]
      defaultWriteFormat         <- arbitrary[String]
      defaultRejectedWriteFormat <- arbitrary[String]
      defaultAuditWriteFormat    <- arbitrary[String]
      csvOutput                  <- arbitrary[Boolean]
      csvOutputExt               <- arbitrary[String]
      privacyOnly                <- arbitrary[Boolean]
      emptyIsNull                <- arbitrary[Boolean]
      loader                     <- arbitrary[String]
      rowValidatorClass          <- arbitrary[String]
      treeValidatorClass         <- arbitrary[String]
      loadStrategyClass <- Gen.oneOf(
        IngestionNameStrategy.getClass.getName.replace("$", ""),
        IngestionTimeStrategy.getClass.getName.replace("$", "")
      )
      grouped                 <- arbitrary[Boolean]
      groupedMax              <- arbitrary[Int]
      scd2StartTimestamp      <- arbitrary[String]
      scd2EndTimestamp        <- arbitrary[String]
      area                    <- arbitrary[Area]
      hadoop                  <- arbitrary[Map[String, String]]
      connections             <- arbitrary[Map[String, Connection]]
      jdbcEngines             <- arbitrary[Map[String, JdbcEngine]]
      privacy                 <- arbitrary[Privacy]
      root                    <- arbitrary[String]
      internal                <- Gen.option(arbitrary[Internal])
      accessPolicies          <- arbitrary[AccessPolicies]
      sparkScheduling         <- arbitrary[SparkScheduling]
      udfs                    <- Gen.option(arbitrary[String])
      expectations            <- arbitrary[ExpectationsConfig]
      sqlParameterPattern     <- arbitrary[String]
      rejectAllOnError        <- arbitrary[Boolean]
      rejectMaxRecords        <- arbitrary[Int]
      maxParCopy              <- arbitrary[Int]
      kafka                   <- arbitrary[KafkaConfig]
      dsvOptions              <- arbitrary[Map[String, String]]
      rootServe               <- Gen.option(arbitrary[String])
      forceViewPattern        <- arbitrary[String]
      forceDomainPattern      <- arbitrary[String]
      forceTablePattern       <- arbitrary[String]
      forceJobPattern         <- arbitrary[String]
      forceTaskPattern        <- arbitrary[String]
      useLocalFileSystem      <- arbitrary[Boolean]
      sessionDurationServe    <- arbitrary[Long]
      database                <- arbitrary[String]
      tenant                  <- arbitrary[String]
      connectionRef           <- arbitrary[String]
      schedulePresets         <- arbitrary[Map[String, String]]
      maxParTask              <- arbitrary[Int]
      refs                    <- arbitrary[List[Ref]]
      dagRef                  <- Gen.option(arbitrary[DagRef])
      forceHalt               <- arbitrary[Boolean]
      jobIdEnvName            <- Gen.option(arbitrary[String])
      archiveTablePattern     <- arbitrary[String]
      archiveTable            <- arbitrary[Boolean]
      version                 <- arbitrary[String]
      autoExportSchema        <- arbitrary[Boolean]
      longJobTimeoutMs        <- arbitrary[Long]
      shortJobTimeoutMs       <- arbitrary[Long]
      createSchemaIfNotExists <- arbitrary[Boolean]
      http                    <- arbitrary[Http]
      timezone                <- arbitrary[TimeZone]
    } yield AppConfig(
      env = env,
      datasets = datasets,
      incoming = incoming,
      dags = dags,
      writeStrategies = writeStrategies,
      metadata = metadata,
      metrics = metrics,
      validateOnLoad = validateOnLoad,
      audit = audit,
      archive = archive,
      sinkReplayToFile = sinkReplayToFile,
      lock = lock,
      defaultWriteFormat = defaultWriteFormat,
      defaultRejectedWriteFormat = defaultRejectedWriteFormat,
      defaultAuditWriteFormat = defaultAuditWriteFormat,
      csvOutput = csvOutput,
      csvOutputExt = csvOutputExt,
      privacyOnly = privacyOnly,
      emptyIsNull = emptyIsNull,
      loader = loader,
      rowValidatorClass = rowValidatorClass,
      treeValidatorClass = treeValidatorClass,
      loadStrategyClass = loadStrategyClass,
      grouped = grouped,
      groupedMax = groupedMax,
      scd2StartTimestamp = scd2StartTimestamp,
      scd2EndTimestamp = scd2EndTimestamp,
      area = area,
      hadoop = hadoop,
      connections = connections,
      jdbcEngines = jdbcEngines,
      privacy = privacy,
      root = root,
      internal = internal,
      accessPolicies = accessPolicies,
      sparkScheduling = sparkScheduling,
      udfs = udfs,
      expectations = expectations,
      sqlParameterPattern = sqlParameterPattern,
      rejectAllOnError = rejectAllOnError,
      rejectMaxRecords = rejectMaxRecords,
      maxParCopy = maxParCopy,
      kafka = kafka,
      dsvOptions = dsvOptions,
      rootServe = rootServe,
      forceViewPattern = forceViewPattern,
      forceDomainPattern = forceDomainPattern,
      forceTablePattern = forceTablePattern,
      forceJobPattern = forceJobPattern,
      forceTaskPattern = forceTaskPattern,
      useLocalFileSystem = useLocalFileSystem,
      sessionDurationServe = sessionDurationServe,
      database = database,
      tenant = tenant,
      connectionRef = connectionRef,
      schedulePresets = schedulePresets,
      maxParTask = maxParTask,
      refs = refs,
      dagRef = dagRef,
      forceHalt = forceHalt,
      jobIdEnvName = jobIdEnvName,
      archiveTablePattern = archiveTablePattern,
      archiveTable = archiveTable,
      version = version,
      autoExportSchema = autoExportSchema,
      longJobTimeoutMs = longJobTimeoutMs,
      shortJobTimeoutMs = shortJobTimeoutMs,
      createSchemaIfNotExists = createSchemaIfNotExists,
      http = http,
      timezone = timezone
    )
  }

  implicit val config: Arbitrary[Config] = Arbitrary {
    val arbConfigEntry = for {
      key   <- Gen.oneOf('a' to 'z').map(_.toString)
      value <- arbitrary[String]
    } yield key -> value
    Gen.mapOf[String, String](arbConfigEntry).map(m => ConfigFactory.parseMap(m.asJava))
  }

  implicit val sparkConf: Arbitrary[SparkConf] = Arbitrary {
    for {
      sparkAppId <- arbitrary[String]
    } yield {
      new SparkConf(false).set("spark.app.id", sparkAppId)
    }
  }

  implicit val settings: Arbitrary[Settings] = Arbitrary {
    for {
      appConfig   <- arbitrary[AppConfig]
      sparkConfig <- arbitrary[Config]
      extractConf <- arbitrary[Config]
      jobConf     <- arbitrary[SparkConf]
    } yield Settings(
      appConfig = appConfig,
      sparkConfig = sparkConfig,
      extraConf = extractConf,
      jobConf = jobConf
    )
  }

  implicit val applicationDesc: Arbitrary[ApplicationDesc] = Arbitrary {
    for {
      appConfig <- arbitrary[AppConfig]
    } yield ApplicationDesc(latestSchemaVersion, application = appConfig)
  }

  implicit val externalDomain: Arbitrary[ExternalDomain] = Arbitrary {
    for {
      name   <- arbitrary[String]
      tables <- arbitrary[List[String]]
    } yield ExternalDomain(name = name, tables = tables)
  }

  implicit val externalDatabase: Arbitrary[ExternalDatabase] = Arbitrary {
    for {
      project <- arbitrary[String]
      domains <- Gen.option(arbitrary[List[ExternalDomain]])
    } yield ExternalDatabase(project = project, domains = domains)
  }

  implicit val externalSource: Arbitrary[ExternalSource] = Arbitrary {
    for {
      externalDatabases <- Gen.option(arbitrary[List[ExternalDatabase]])
    } yield ExternalSource(projects = externalDatabases)
  }

  implicit val externalDesc: Arbitrary[ExternalDesc] = Arbitrary {
    for {
      external <- arbitrary[ExternalSource]
    } yield ExternalDesc(latestSchemaVersion, external = external)
  }

  implicit val attributeDesc: Arbitrary[AttributeDesc] = Arbitrary {
    for {
      name         <- arbitrary[String]
      comment      <- arbitrary[String]
      accessPolicy <- Gen.option(arbitrary[String])
    } yield AttributeDesc(name = name, comment = comment, accessPolicy = accessPolicy)
  }

  implicit val path: Arbitrary[Path] = Arbitrary {
    Gen.oneOf("/tmp", "relativeFolder/subfolder", "myFolder").map(new Path(_))
  }

  implicit val autoTaskDesc: Arbitrary[AutoTaskDesc] = Arbitrary {
    for {
      name           <- arbitrary[String]
      sql            <- Gen.option(arbitrary[String])
      database       <- Gen.option(arbitrary[String])
      domain         <- arbitrary[String]
      table          <- arbitrary[String]
      partition      <- arbitrary[List[String]]
      presql         <- arbitrary[List[String]]
      postsql        <- arbitrary[List[String]]
      sink           <- Gen.option(arbitrary[AllSinks])
      rls            <- arbitrary[List[RowLevelSecurity]]
      expectations   <- arbitrary[List[ExpectationItem]]
      acl            <- arbitrary[List[AccessControlEntry]]
      comment        <- Gen.option(arbitrary[String])
      freshness      <- Gen.option(arbitrary[Freshness])
      attributesDesc <- arbitrary[List[AttributeDesc]]
      python         <- Gen.option(arbitrary[Path])
      tags           <- arbitrary[List[String]].map(_.toSet)
      schedule       <- Gen.option(arbitrary[String])
      dagRef         <- Gen.option(arbitrary[String])
      taskTimeoutMs  <- Gen.option(arbitrary[Long])
      parseSQL       <- Gen.option(arbitrary[Boolean])
    } yield {
      val autoTask = AutoTaskDesc(
        name = name,
        sql = sql,
        database = database,
        domain = domain,
        table = table,
        presql = presql,
        postsql = postsql,
        sink = sink,
        rls = rls,
        expectations = expectations,
        acl = acl,
        comment = comment,
        freshness = freshness,
        attributesDesc = attributesDesc,
        python = python,
        tags = tags,
        schedule = schedule,
        dagRef = dagRef,
        taskTimeoutMs = taskTimeoutMs,
        parseSQL = parseSQL
      )
      autoTask.copy(
        // fill with default value in order to match with deserialization
        sql = Some(autoTask.getSql()),
        database = Some(
          autoTask.database.getOrElse("forcedDatabase")
        ) // use fallback database because we don't have access to settings here in order to get the value
      )
    }
  }

  implicit val taskDesc: Arbitrary[TaskDesc] = Arbitrary {
    for {
      task <- arbitrary[AutoTaskDesc]
    } yield TaskDesc(latestSchemaVersion, task)
  }

  implicit val autoJobDesc: Arbitrary[AutoJobDesc] = Arbitrary {
    for {
      name    <- arbitrary[String]
      tasks   <- arbitrary[List[AutoTaskDesc]].map(_.take(maxElementInCollections))
      comment <- Gen.option(arbitrary[String])
      default <- Gen.option(arbitrary[AutoTaskDesc])
    } yield AutoJobDesc(name = name, tasks = tasks, comment = comment, default = default)
  }

  implicit val transformDesc: Arbitrary[TransformDesc] = Arbitrary {
    for {
      transform <- arbitrary[AutoJobDesc]
    } yield TransformDesc(latestSchemaVersion, transform = transform)
  }

  implicit val tableDesc: Arbitrary[TableDesc] = Arbitrary {
    for {
      table <- arbitrary[Schema]
    } yield TableDesc(latestSchemaVersion, table)
  }

  implicit val tablesDesc: Arbitrary[TablesDesc] = Arbitrary {
    for {
      tables <- arbitrary[List[Schema]].map(_.take(maxElementInCollections))
    } yield TablesDesc(latestSchemaVersion, tables)
  }

  implicit val primitiveType: Arbitrary[PrimitiveType] = Arbitrary {
    Gen.oneOf(PrimitiveType.primitiveTypes)
  }

  implicit val validType: Arbitrary[Type] = Arbitrary {
    for {
      name          <- arbitrary[String]
      primitiveType <- arbitrary[PrimitiveType]
      zone          <- Gen.option(arbitrary[String])
      sample        <- Gen.option(arbitrary[String])
      comment       <- Gen.option(arbitrary[String])
      pattern <-
        if (primitiveType == PrimitiveType.boolean) Gen.oneOf("T<-TF->F", "O<-TF->N", "Y<-TF->N")
        else arbitrary[Pattern].map(_.toString)
      ddlMapping <- Gen.option(arbitrary[Map[String, String]])
    } yield {
      Type(
        name = name,
        pattern = pattern,
        primitiveType = primitiveType,
        zone = zone,
        sample = sample,
        comment = comment,
        ddlMapping = ddlMapping
      )
    }
  }

  implicit val typeDesc: Arbitrary[TypesDesc] = Arbitrary {
    for {
      types <- arbitrary[List[Type]].map(_.take(maxElementInCollections))
    } yield TypesDesc(latestSchemaVersion, types = types)
  }

  implicit val envDesc: Arbitrary[EnvDesc] = Arbitrary {
    for {
      envMap <- arbitrary[Map[String, String]]
    } yield EnvDesc(latestSchemaVersion, env = envMap)
  }
}
