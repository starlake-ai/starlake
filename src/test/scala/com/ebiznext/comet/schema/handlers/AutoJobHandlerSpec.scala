package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.{Settings, StorageArea}
import com.ebiznext.comet.schema.model.{AutoJobDesc, AutoTaskDesc, WriteMode}
import com.ebiznext.comet.workflow.IngestionWorkflow
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

class AutoJobHandlerSpec extends TestHelper with BeforeAndAfterAll {

  lazy val pathBusiness = new Path(cometMetadataPath + "/jobs/user.yml")

  lazy val pathGraduateProgramBusiness = new Path(cometMetadataPath + "/jobs/graduateProgram.yml")

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
        "select firstname, lastname, age from user_View where age={{age}}",
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
      val schemaHandler = new SchemaHandler(metadataStorageHandler)

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())
      storageHandler.write(businessJobDef, pathBusiness)

      workflow.autoJobRun("user", Some("age=40"))

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
        "select firstname, lastname, age from user_View where age={{age}} and lastname={{lastname}} and firstname={{firstname}}",
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
      val schemaHandler = new SchemaHandler(storageHandler)

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())
      storageHandler.write(businessJobDef, pathBusiness)

      workflow.autoJobRun("user", Some("age=25, lastname='Doe', firstname='John'"))

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
        "select firstname, lastname, age from user_View",
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
      val schemaHandler = new SchemaHandler(storageHandler)

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())
      storageHandler.write(businessJobDef, pathBusiness)

      workflow.autoJobRun("user")

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
        "select concatWithSpace(firstname, lastname) as fullName from user_View",
        "user",
        "user",
        WriteMode.OVERWRITE,
        area = Some(StorageArea.fromString("business"))
      )
      val businessJob =
        AutoJobDesc(
          "fullName",
          List(businessTask1),
          None,
          Some("parquet"),
          Some(false),
          udf = Some("com.ebiznext.comet.udf.TestUdf"),
          views = Some(Map("user_View" -> "accepted/user"))
        )
      val schemaHandler = new SchemaHandler(storageHandler)

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)

      storageHandler.write(businessJobDef, pathBusiness)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())

      workflow.autoJobRun("fullName")

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
        "SELECT * FROM graduate_agg_view",
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
      val schemaHandler = new SchemaHandler(storageHandler)

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())
      storageHandler.write(businessJobDef, pathGraduateProgramBusiness)

      workflow.autoJobRun("graduateProgram", Some("school='UC_Berkeley'"))

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
}
