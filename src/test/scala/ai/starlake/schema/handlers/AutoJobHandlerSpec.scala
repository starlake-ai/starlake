package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig, BigQuerySparkJob}
import ai.starlake.job.transform.{AutoTask, TransformConfig}
import ai.starlake.schema.generator.TaskViewDependency
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.repackaged.gcs.com.google.auth.oauth2.GoogleCredentials
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

class AutoJobHandlerSpec extends TestHelper with BeforeAndAfterAll {

  lazy val pathBusiness = new Path(starlakeMetadataPath + "/transform/user/user.sl.yml")

  lazy val pathConfigBusiness = new Path(
    starlakeMetadataPath + "/transform/user/_config.sl.yml"
  )

  lazy val pathGraduateProgramBusiness = new Path(
    starlakeMetadataPath + "/transform/user/graduateProgram.sl.yml"
  )

  lazy val pathGraduateDatasetProgramBusiness = new Path(
    starlakeDatasetsPath + "/business/graduateProgram/output"
  )

  lazy val pathUserDatasetBusiness = new Path(starlakeDatasetsPath + "/business/user/user")

  lazy val pathUserAccepted = new Path(starlakeDatasetsPath + "/accepted/user")

  lazy val pathGraduateProgramAccepted = new Path(
    starlakeDatasetsPath + "/accepted/graduateProgram"
  )

  lazy val metadataPath = new Path(starlakeMetadataPath)

  override def beforeAll(): Unit = {
    new WithSettings() {
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
  }

  new WithSettings() {
    "trigger AutoJob by passing parameters on SQL statement" should "generate a dataset in business" in {

      val userView = s"${settings.appConfig.datasets}/accepted/user"
      val businessTask1 = AutoTaskDesc(
        name = "",
        sql = Some(
          s"with user_view as (select * from parquet.`$userView`) select firstname, lastname, age from user_view where age={{age}}"
        ),
        database = None,
        domain = "user",
        table = "user",
        write = Some(WriteMode.OVERWRITE),
        python = None,
        merge = None,
        sink = Some(FsSink().toAllSinks())
      )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessTask1)
      storageHandler.write(businessJobDef, pathBusiness)

      val configJob =
        AutoJobDesc(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(configJob)
      storageHandler.write(configJobDef, pathConfigBusiness)

      val schemaHandler =
        new SchemaHandler(storageHandler, Map("age" -> "40"))

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)

      workflow.autoJob(TransformConfig("user.user"))

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

    "Extract file and view dependencies" should "work" in {

      val userView = s"${settings.appConfig.datasets}/accepted/user"
      logger.info("************userView:" + userView)
      val businessTask1 = AutoTaskDesc(
        name = "user",
        sql = Some(
          s"with user_view as (select * from parquet.`$userView`) select firstname, lastname, age from user_view where age={{age}} and lastname={{lastname}} and firstname={{firstname}}"
        ),
        database = None,
        domain = "user",
        table = "user",
        write = Some(WriteMode.OVERWRITE),
        expectations = Map("uniqFirstname" -> "expect_column_is_unique(firstname)"),
        python = None,
        merge = None
      )
      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessTask1)
      storageHandler.write(businessJobDef, pathBusiness)

      val configJob =
        AutoJobDesc(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(configJob)
      storageHandler.write(configJobDef, pathConfigBusiness)

      val schemaHandler = new SchemaHandler(storageHandler)

      val tasks = AutoTask.unauthenticatedTasks(true)(settings, storageHandler, schemaHandler)
      val deps = TaskViewDependency.dependencies(tasks)(schemaHandler)
      deps.map(_.parentRef) should contain theSameElementsAs List(
        "parquet." + userView,
        "user_view"
      )
    }

    "trigger AutoJob by passing three parameters on SQL statement" should "generate a dataset in business" in {

      val userView = s"${settings.appConfig.datasets}/accepted/user"
      val businessTask1 = AutoTaskDesc(
        name = "user",
        sql = Some(
          s"with user_view as (select * from parquet.`$userView`) select firstname, lastname, age from user_View where age={{age}} and lastname={{lastname}} and firstname={{firstname}}"
        ),
        database = None,
        domain = "user",
        table = "user",
        write = Some(WriteMode.OVERWRITE),
        expectations = Map("uniqFirstname" -> "expect_column_is_unique(firstname)"),
        python = None,
        merge = None,
        sink = Some(FsSink().toAllSinks())
      )
      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessTask1)
      storageHandler.write(businessJobDef, pathBusiness)

      val configJob =
        AutoJobDesc(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(configJob)
      storageHandler.write(configJobDef, pathConfigBusiness)

      val schemaHandler = new SchemaHandler(
        storageHandler,
        Map("age" -> "25", "lastname" -> "'Doe'", "firstname" -> "'John'")
      )

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)
      workflow.autoJob(
        TransformConfig("user.user")
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

      val userView = s"${settings.appConfig.datasets}/accepted/user"
      val businessTask1 = AutoTaskDesc(
        name = "user",
        sql = Some(
          s"with user_view as (select * from parquet.`$userView`) select firstname, lastname, age from user_view"
        ),
        database = None,
        domain = "user",
        table = "user",
        write = Some(WriteMode.OVERWRITE),
        python = None,
        merge = None,
        sink = Some(FsSink().toAllSinks())
      )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessTask1)
      storageHandler.write(businessJobDef, pathBusiness)

      val schemaHandler = new SchemaHandler(storageHandler)

      val configJob =
        AutoJobDesc(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(configJob)
      storageHandler.write(configJobDef, pathConfigBusiness)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)

      workflow.autoJob(TransformConfig("user.user"))

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

      val userView = s"${settings.appConfig.datasets}/accepted/user"
      val businessTask1 = AutoTaskDesc(
        name = "user",
        sql = Some(
          s"with user_view as (select * from parquet.`$userView`) select concatWithSpace(firstname, lastname) as fullName from user_View"
        ),
        database = None,
        domain = "user",
        table = "user",
        write = Some(WriteMode.OVERWRITE),
        python = None,
        merge = None,
        sink = Some(FsSink().toAllSinks())
      )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessTask1)
      storageHandler.write(businessJobDef, pathBusiness)

      val configJob =
        AutoJobDesc(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(configJob)
      storageHandler.write(configJobDef, pathConfigBusiness)

      val schemaHandler = new SchemaHandler(storageHandler)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)

      workflow.autoJob(TransformConfig("user.user"))

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
        name = "graduateProgram",
        sql = Some(
          s"SELECT * FROM graduate_agg_view"
        ),
        database = None,
        domain = "graduateProgram",
        table = "output",
        write = Some(WriteMode.OVERWRITE),
        presql = List(
          s"""
            |create or replace temporary view graduate_agg_view as
            |      select degree, department,
            |      school
            |      from parquet.`${settings.appConfig.datasets}/accepted/graduateProgram`
            |      where school={{school}}
            |""".stripMargin
        ),
        python = None,
        merge = None,
        sink = Some(FsSink().toAllSinks())
      )
      val configJob =
        AutoJobDesc(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(configJob)
      storageHandler.write(configJobDef, pathConfigBusiness)

      val businessTaskDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessTask1)
      storageHandler.write(businessTaskDef, pathGraduateProgramBusiness)

      val schemaHandler = new SchemaHandler(storageHandler, Map("school" -> "'UC_Berkeley'"))
      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)

      workflow.autoJob(TransformConfig("graduateProgram.graduateProgram"))

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
  }

  "BQ Business Job Definition" should "Prepare correctly against BQ" in {
    val bqConfiguration: Config = {
      val config = ConfigFactory
        .parseString("""
            |connectionRef = "bigquery"
            |""".stripMargin)
      val result = config.withFallback(super.testConfiguration)
      result
    }
    val credentials: GoogleCredentials = GoogleCredentials.getApplicationDefault()

    new WithSettings(bqConfiguration) {
      val businessTask1 = AutoTaskDesc(
        name = "",
        sql = Some("select * from domain"),
        database = None,
        domain = "DOMAIN",
        table = "TABLE",
        write = Some(WriteMode.OVERWRITE),
        partition = List("comet_year", "comet_month"),
        presql = Nil,
        postsql = Nil,
        None,
        rls = List(RowLevelSecurity("myrls", "TRUE", Set("user:hayssam.saleh@ebiznext.com"))),
        python = None,
        merge = None
      )

      val sink = businessTask1.sink.map(_.asInstanceOf[BigQuerySink])

      val config = BigQueryLoadConfig(
        connectionRef = None,
        outputTableId = Some(
          BigQueryJobBase.extractProjectDatasetAndTable(
            businessTask1.database,
            businessTask1.domain,
            businessTask1.table
          )
        ),
        sourceFormat = "parquet",
        createDisposition = "CREATE_IF_NEEDED",
        writeDisposition = "WRITE_TRUNCATE",
        outputPartition = sink.flatMap(_.timestamp),
        outputClustering = sink.flatMap(_.clustering).getOrElse(Nil),
        days = sink.flatMap(_.days),
        requirePartitionFilter = sink.flatMap(_.requirePartitionFilter).getOrElse(false),
        rls = businessTask1.rls,
        outputDatabase = None
      )
      val job = new BigQuerySparkJob(config)
      val conf = job.prepareConf()

      conf.get(
        BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION.getKey()
      ) shouldEqual "WRITE_TRUNCATE"
      conf.get(
        BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION.getKey()
      ) shouldEqual "CREATE_IF_NEEDED"

      val delStatement = "DROP ALL ROW ACCESS POLICIES ON `DOMAIN.TABLE`"
      val createStatement =
        """
          | CREATE ROW ACCESS POLICY
          |  myrls
          | ON
          |  `DOMAIN.TABLE`
          | GRANT TO
          |  ("user:hayssam.saleh@ebiznext.com")
          | FILTER USING
          |  (TRUE)
          |""".stripMargin
      val result = job
        .prepareRLS()
        .map(_.replaceAll("`.*`", "`DOMAIN.TABLE`")) // we remove the computed project id
      result should contain theSameElementsInOrderAs List(delStatement, createStatement)
    }
  }
}
