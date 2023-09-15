package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract._
import ai.starlake.job.ingest.WatchConfig
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.handlers.{SchemaHandler, SimpleLauncher}
import ai.starlake.schema.model._
import ai.starlake.utils.JsonSerializer
import ai.starlake.workflow.IngestionWorkflow
import com.google.cloud.bigquery.{BigQueryOptions, TableId}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

import java.time.Instant

class BigQueryNativeJobSpec extends TestHelper with BeforeAndAfterAll {
  val bigquery = BigQueryOptions.newBuilder().build().getService()
  override def beforeAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      bigquery.delete(TableId.of("bqtest", "account"))
      bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }
  override def afterAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "account"))
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }

  val bigQueryConfiguration: Config = {
    val config = ConfigFactory.parseString("""
        |udfs: ""
        |
        |connectionRef: bqtest
        |connections.bqtest {
        |  type = "bigquery"
        |  options {
        |    gcsBucket: starlake-app
        |    authType: APPLICATION_DEFAULT
        |    #authType: SERVICE_ACCOUNT_JSON_KEYFILE
        |    #jsonKeyfile: "/Users/me/.gcloud/keys/my-key.json"
        |  }
        |}
        |""".stripMargin)
    val result = config.withFallback(super.testConfiguration)
    result
  }
  new WithSettings(bigQueryConfiguration) {
    "Ingest to BigQuery" should "be ingested and stored in a BigQuery table" in {
      if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
        import org.slf4j.impl.StaticLoggerBinder
        val binder = StaticLoggerBinder.getSingleton
        logger.debug(binder.getLoggerFactory.toString)
        logger.debug(binder.getLoggerFactoryClassStr)

        new SpecTrait(
          domainOrJobFilename = "bqtest.sl.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.sl.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          cleanMetadata
          cleanDatasets

          logger.info(settings.appConfig.datasets)
          loadPending
        }
        val tableFound =
          Option(bigquery.getTable(TableId.of("bqtest", "account"))).isDefined
        tableFound should be(true)

      }
    }
    "Secure BigQuery Tables" should "should set policies in tables" in {
      if (false && sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
        import org.slf4j.impl.StaticLoggerBinder
        val binder = StaticLoggerBinder.getSingleton
        logger.debug(binder.getLoggerFactory.toString)
        logger.debug(binder.getLoggerFactoryClassStr)

        new SpecTrait(
          domainOrJobFilename = "bqtest.sl.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.sl.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          cleanMetadata
          cleanDatasets

          logger.info(settings.appConfig.datasets)
          secure(WatchConfig())
        }
        val tableFound =
          Option(bigquery.getTable(TableId.of("bqtest", "account"))).isDefined
        tableFound should be(true)

      }
    }

    "Native BigQuery AutoJob" should "succeed" in {
      if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
        new SpecTrait(
          domainOrJobFilename = "bqtest.sl.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.sl.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          val businessTask1 = AutoTaskDesc(
            "",
            Some("select * except(code0000) from bqtest.account"),
            None,
            "bqtest",
            "jobresult",
            Some(WriteMode.OVERWRITE),
            sink = Some(
              BigQuerySink(connectionRef = None).toAllSinks()
            ),
            python = None,
            merge = None,
            parseSQL = Some(false)
          )
          val businessTaskDef = mapper
            .writer()
            .withAttribute(classOf[Settings], settings)
            .writeValueAsString(businessTask1)
          val pathBusiness =
            new Path(starlakeMetadataPath + "/transform/bqtest/bqjobtest.sl.yml")
          storageHandler.write(businessTaskDef, pathBusiness)

          val configJob =
            AutoJobDesc(
              "",
              Nil
            )

          val configJobDef = mapper
            .writer()
            .withAttribute(classOf[Settings], settings)
            .writeValueAsString(configJob)
          val pathConfigBusiness =
            new Path(starlakeMetadataPath + "/transform/bqtest/_config.sl.yml")
          storageHandler.write(configJobDef, pathConfigBusiness)

          val schemaHandler = new SchemaHandler(storageHandler)

          val workflow =
            new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())
          val config = TransformConfig("bqtest.bqjobtest")
          workflow.autoJob(config) should be(true)
          workflow.autoJob(config.copy(interactive = Some("json"))) should be(true)
          workflow.autoJob(config.copy(interactive = Some("csv"))) should be(true)
          workflow.autoJob(config.copy(interactive = Some("table"))) should be(true)
        }
      }
    }
    "Extract Table infos" should "succeed" in {
      val logTime = java.sql.Timestamp.from(Instant.now)
      val start = System.currentTimeMillis()
      val infos = BigQueryInfo.extractInfo(BigQueryTablesConfig())
      val end = System.currentTimeMillis()
      println((end - start) / 1000)
      val datasetInfos = infos.map(_._1).map(BigQueryDatasetInfo(_, logTime))
      val tableInfos = infos.flatMap(_._2).map(BigQueryTableInfo(_, logTime))
      println(JsonSerializer.serializeObject(datasetInfos))
      println(JsonSerializer.serializeObject(tableInfos))
      val config = BigQueryTablesConfig()
      BigQueryTableInfo.sink(config)
    }
    "Freshness of Table" should "return list of warning & errors" in {
      if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
        import org.slf4j.impl.StaticLoggerBinder
        val binder = StaticLoggerBinder.getSingleton
        logger.debug(binder.getLoggerFactory.toString)
        logger.debug(binder.getLoggerFactoryClassStr)

        new SpecTrait(
          domainOrJobFilename = "bqtest.sl.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.sl.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          val config = BigQueryTablesConfig(tables = Map("bqtest" -> List("account")))
          val result = BigQueryFreshnessInfo.freshness(config)
          val json = JsonSerializer.serializeObject(result)
          println(json)

        }
      }
    }
  }
}
