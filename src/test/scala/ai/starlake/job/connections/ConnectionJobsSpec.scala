package ai.starlake.job.connections

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.handlers.{SchemaHandler, SimpleLauncher}
import ai.starlake.schema.model.{AutoJobDesc, AutoTaskDesc, JdbcSink, WriteMode}
import ai.starlake.workflow.IngestionWorkflow
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode

class ConnectionJobsSpec extends TestHelper {
  lazy val pathBusiness = new Path(cometMetadataPath + "/jobs/user.comet.yml")
  new WithSettings() {
    "JDBC 2 JDBC Connection" should "succeed" in {
      val connection = "test-h2"

      import sparkSession.implicits._
      val usersDF = Seq(
        ("John", "Doe", 10),
        ("Sam", "Hal", 20),
        ("Martin", "Odersky", 30)
      ).toDF("firstname", "lastname", "age")
      val usersOptions = settings.comet.connections(connection).options + ("dbtable" -> "users")
      usersDF.write.format("jdbc").options(usersOptions).mode(SaveMode.Overwrite).save()

      val businessTask1 = AutoTaskDesc(
        "",
        Some("select firstname, lastname from user_View where age = {{age}}"),
        "user",
        "userout",
        WriteMode.OVERWRITE,
        sink = Some(JdbcSink(connection = connection))
      )
      val businessJob =
        AutoJobDesc(
          "user",
          List(businessTask1),
          Nil,
          None,
          Some("parquet"),
          Some(false),
          views = Some(Map("user_View" -> s"jdbc:$connection:select * from users"))
        )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessJob)

      storageHandler.write(businessJobDef, pathBusiness)

      val schemaHandler = new SchemaHandler(metadataStorageHandler, Map("age" -> "10"))

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())

      workflow.autoJob(TransformConfig("user"))

      val userOutOptions = settings.comet.connections(connection).options + ("dbtable" -> "userout")
      sparkSession.read.format("jdbc").options(userOutOptions).load.collect() should have size 1
    }
  }
}
