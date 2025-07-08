package ai.starlake.job.connections

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.model.{AutoJobInfo, AutoTaskInfo, JdbcSink, WriteStrategy}
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode

class ConnectionJobsSpec extends TestHelper {
  lazy val pathBusiness = new Path(starlakeMetadataPath + "/transform/myusers/myusers.sl.yml")
  val pgConfiguration: Config = {
    val config = ConfigFactory.parseString("""
                                             |connectionRef: "test-pg"
                                             |""".stripMargin)
    val result = config.withFallback(super.testConfiguration)
    result
  }
  new WithSettings(pgConfiguration) {
    "JDBC 2 JDBC Connection" should "succeed" in {
      val connection = "test-pg"
      val session = sparkSession
      import session.implicits._
      val usersDF = Seq(
        ("John", "Doe", 10),
        ("Sam", "Hal", 20),
        ("Martin", "Odersky", 30)
      ).toDF("firstname", "lastname", "age")
      val usersOptions =
        settings.appConfig.connections(connection).options + ("dbtable" -> "myusers.myusers")

      JdbcDbUtils.withJDBCConnection(usersOptions) { conn =>
        val statement = conn.createStatement()
        statement.execute("CREATE SCHEMA myusers")
        statement.execute("CREATE TABLE myusers.myusers(firstname TEXT, lastname TEXT, age INT)")
      }
      usersDF.write.format("jdbc").options(usersOptions).mode(SaveMode.Overwrite).save()
      println(pgContainer.jdbcUrl)
      val businessTask1 = AutoTaskInfo(
        name = "",
        sql = Some(
          "(with user_View as (select * from myusers.myusers) select firstname, lastname from user_View where age = {{age}})"
        ),
        database = None,
        domain = "myusers",
        table = "userout",
        sink = Some(JdbcSink(connectionRef = Some(connection)).toAllSinks()),
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite)
      )
      val businessJob =
        AutoJobInfo(
          name = "myusers",
          tasks = List(businessTask1)
        )

      val businessTaskDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessTask1)

      storageHandler.write(businessTaskDef, pathBusiness)

      val schemaHandler = settings.schemaHandler(Map("age" -> "10"))

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)

      workflow.autoJob(TransformConfig("myusers.myusers"))

      val userOutOptions =
        settings.appConfig.connections(connection).options + ("dbtable" -> "myusers.userout")
      sparkSession.read.format("jdbc").options(userOutOptions).load.collect() should have size 1
    }
  }
}
