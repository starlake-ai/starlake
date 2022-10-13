package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.config.{Settings, StorageArea}
import ai.starlake.job.sink.bigquery.{BigQueryLoadConfig, BigQuerySparkJob}
import ai.starlake.schema.model._
import ai.starlake.workflow.{IngestionWorkflow, TransformConfig}
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

class AutoJobHandlerSpec extends TestHelper with BeforeAndAfterAll {

  lazy val pathBusiness = new Path(cometMetadataPath + "/jobs/user.comet.yml")

  lazy val pathGraduateProgramBusiness = new Path(
    cometMetadataPath + "/jobs/graduateProgram.comet.yml"
  )

  lazy val pathGraduateDatasetProgramBusiness = new Path(
    cometDatasetsPath + "/business/graduateProgram/output"
  )

  lazy val pathUserDatasetBusiness = new Path(cometDatasetsPath + "/business/user/user")

  lazy val pathUserAccepted = new Path(cometDatasetsPath + "/accepted/user")

  lazy val pathGraduateProgramAccepted = new Path(cometDatasetsPath + "/accepted/graduateProgram")

  lazy val metadataPath = new Path(cometMetadataPath)

  override def beforeAll(): Unit = {
    sparkSession.read
      .option("inferSchema", "true")
      .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))
      .write
      .mode("overwrite")
      .parquet(pathUserAccepted.toString)

    sparkSession.read
      .option("inferSchema", "true")
      .json(getResPath("/expected/datasets/accepted/DOMAIN/graduateProgram.json"))
      .write
      .mode("overwrite")
      .parquet(pathGraduateProgramAccepted.toString)
  }

  new WithSettings() {
    "trigger AutoJob by passing parameters on SQL statement" should "generate a dataset in business" in {

      val businessTask1 = AutoTaskDesc(
        None,
        Some("select firstname, lastname, age from {{view}} where age=${age}"),
        "user",
        "user",
        WriteMode.OVERWRITE,
        area = Some(StorageArea.fromString("business"))
      )
      val businessJob =
        AutoJobDesc(
          "user",
          List(businessTask1),
          None,
          Some("parquet"),
          Some(false),
          views = Some(Map("user_View" -> "accepted/user"))
        )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)
      storageHandler.write(businessJobDef, pathBusiness)

      val schemaHandler = new SchemaHandler(metadataStorageHandler)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())

      workflow.autoJob(TransformConfig("user", Map("view" -> "user_View", "age" -> "40")))

      val result = sparkSession.read
        .load(pathUserDatasetBusiness.toString)
        .select("firstname", "lastname", "age")
        .take(2)

      result.length shouldBe 2

      result
        .map(r => (r.getString(0), r.getString(1), r.getLong(2)))
        .toList should contain allElementsOf List(
        ("test3", "test4", 40),
        ("Elon", "Musk", 40)
      )
    }

    "trigger AutoJob by passing three parameters on SQL statement" should "generate a dataset in business" in {

      val businessTask1 = AutoTaskDesc(
        None,
        Some(
          "select firstname, lastname, age from user_View where age={{age}} and lastname={{lastname}} and firstname={{firstname}}"
        ),
        "user",
        "user",
        WriteMode.OVERWRITE,
        area = Some(StorageArea.fromString("business")),
        assertions = Some(Map("uniqFirstname" -> "isUnique(firstname)"))
      )
      val businessJob =
        AutoJobDesc(
          "user",
          List(businessTask1),
          None,
          Some("parquet"),
          Some(false),
          views = Some(Map("user_View" -> "accepted/user"))
        )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)
      storageHandler.write(businessJobDef, pathBusiness)

      val schemaHandler = new SchemaHandler(storageHandler)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())

      workflow.autoJob(
        TransformConfig("user", Map("age" -> "25", "lastname" -> "'Doe'", "firstname" -> "'John'"))
      )

      val result = sparkSession.read
        .load(pathUserDatasetBusiness.toString)
        .select("firstname", "lastname", "age")
        .take(2)

      result.length shouldBe 1
      result
        .map(r => (r.getString(0), r.getString(1), r.getLong(2)))
        .toList should contain allElementsOf List(
        ("John", "Doe", 25)
      )
    }

    "trigger AutoJob with no parameters on SQL statement" should "generate a dataset in business" in {

      val businessTask1 = AutoTaskDesc(
        None,
        Some("select firstname, lastname, age from user_View"),
        "user",
        "user",
        WriteMode.OVERWRITE,
        area = Some(StorageArea.fromString("business"))
      )
      val businessJob =
        AutoJobDesc(
          "user",
          List(businessTask1),
          None,
          Some("parquet"),
          Some(false),
          views = Some(Map("user_View" -> "accepted/user"))
        )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)
      storageHandler.write(businessJobDef, pathBusiness)

      val schemaHandler = new SchemaHandler(storageHandler)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())

      workflow.autoJob(TransformConfig("user"))

      sparkSession.read
        .load(pathUserDatasetBusiness.toString)
        .select("firstname", "lastname", "age")
        .take(6)
        .map(r => (r.getString(0), r.getString(1), r.getLong(2)))
        .toList should contain allElementsOf List(
        ("John", "Doe", 25),
        ("fred", "abruzzi", 25),
        ("test3", "test4", 40)
      )
    }

    "trigger AutoJob using an UDF" should "generate a dataset in business" in {

      val businessTask1 = AutoTaskDesc(
        None,
        Some("select concatWithSpace(firstname, lastname) as fullName from user_View"),
        "user",
        "user",
        WriteMode.OVERWRITE,
        area = Some(StorageArea.fromString("business"))
      )
      val businessJob =
        AutoJobDesc(
          "user",
          List(businessTask1),
          None,
          Some("parquet"),
          Some(false),
          udf = Some("ai.starlake.udf.TestUdf"),
          views = Some(Map("user_View" -> "accepted/user"))
        )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)

      storageHandler.write(businessJobDef, pathBusiness)

      val schemaHandler = new SchemaHandler(storageHandler)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())

      workflow.autoJob(TransformConfig("user"))

      sparkSession.read
        .load(pathUserDatasetBusiness.toString)
        .select("fullName")
        .take(7)
        .map(r => r.getString(0))
        .toList should contain allElementsOf List(
        "John Doe",
        "fred abruzzi",
        "test3 test4"
      )
    }

    "trigger AutoJob by passing parameters to presql statement" should "generate a dataset in business" in {

      val businessTask1 = AutoTaskDesc(
        None,
        Some("SELECT * FROM graduate_agg_view"),
        "graduateProgram",
        "output",
        WriteMode.OVERWRITE,
        presql = Some(List("""
            |create or replace temporary view graduate_agg_view as
            |      select degree, department,
            |      school
            |      from graduate_View
            |      where school={{school}}
            |""".stripMargin)),
        area = Some(StorageArea.fromString("business"))
      )
      val businessJob =
        AutoJobDesc(
          "graduateProgram",
          List(businessTask1),
          None,
          Some("parquet"),
          Some(false),
          views = Some(Map("graduate_View" -> "accepted/graduateProgram"))
        )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)
      storageHandler.write(businessJobDef, pathGraduateProgramBusiness)

      val schemaHandler = new SchemaHandler(storageHandler)
      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())

      workflow.autoJob(TransformConfig("graduateProgram", Map("school" -> "'UC_Berkeley'")))

      val result = sparkSession.read
        .load(pathGraduateDatasetProgramBusiness.toString)
        .select("*")

      result
        .take(7)
        .map(r => (r.getString(0), r.getString(1), r.getString(2)))
        .toList should contain allElementsOf List(
        ("Masters", "School_of_Information", "UC_Berkeley"),
        ("Masters", "EECS", "UC_Berkeley"),
        ("Ph.D.", "EECS", "UC_Berkeley")
      )

    }

    "BQ Business Job Definition" should "Prepare correctly against BQ" in {
      val businessTask1 = AutoTaskDesc(
        None,
        Some("select * from domain"),
        "DOMAIN",
        "TABLE",
        WriteMode.OVERWRITE,
        Some(List("comet_year", "comet_month")),
        None,
        None,
        None,
        None,
        Some(
          List(RowLevelSecurity("myrls", "TRUE", Set("user:hayssam.saleh@ebiznext.com")))
        )
      )

      val sink = businessTask1.sink.map(_.asInstanceOf[BigQuerySink])

      val config = BigQueryLoadConfig(
        outputDataset = businessTask1.domain,
        outputTable = businessTask1.table,
        sourceFormat = "parquet",
        createDisposition = "CREATE_IF_NEEDED",
        writeDisposition = "WRITE_TRUNCATE",
        location = sink.flatMap(_.location),
        outputPartition = sink.flatMap(_.timestamp),
        outputClustering = sink.flatMap(_.clustering).getOrElse(Nil),
        days = sink.flatMap(_.days),
        requirePartitionFilter = sink.flatMap(_.requirePartitionFilter).getOrElse(false),
        rls = businessTask1.rls
      )
      val job = new BigQuerySparkJob(config)
      val conf = job.prepareConf()

      conf.get(
        BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION.getKey()
      ) shouldEqual "WRITE_TRUNCATE"
      conf.get(
        BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION.getKey()
      ) shouldEqual "CREATE_IF_NEEDED"

      val delStatement = "DROP ALL ROW ACCESS POLICIES ON DOMAIN.TABLE"
      val createStatement =
        """
          | CREATE ROW ACCESS POLICY
          |  myrls
          | ON
          |  DOMAIN.TABLE
          | GRANT TO
          |  ("user:hayssam.saleh@ebiznext.com")
          | FILTER USING
          |  (TRUE)
          |""".stripMargin
      job.prepareRLS() should contain theSameElementsInOrderAs List(delStatement, createStatement)
    }
  }
}
