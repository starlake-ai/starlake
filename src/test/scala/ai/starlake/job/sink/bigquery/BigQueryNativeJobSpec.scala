package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract._
import ai.starlake.job.ingest.WatchConfig
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.generator.BigQueryTablesConfig
import ai.starlake.schema.handlers.{SchemaHandler, SimpleLauncher}
import ai.starlake.schema.model._
import ai.starlake.utils.JsonSerializer
import ai.starlake.workflow.IngestionWorkflow
import com.google.cloud.bigquery.{BigQueryOptions, TableId}
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

  "Ingest to BigQuery" should "be ingested and stored in a BigQuery table" in {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      import org.slf4j.impl.StaticLoggerBinder
      val binder = StaticLoggerBinder.getSingleton
      logger.debug(binder.getLoggerFactory.toString)
      logger.debug(binder.getLoggerFactoryClassStr)

      new WithSettings() {
        new SpecTrait(
          domainOrJobFilename = "bqtest.comet.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.comet.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          cleanMetadata
          cleanDatasets

          logger.info(settings.comet.datasets)
          loadPending
        }
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

      new WithSettings() {
        new SpecTrait(
          domainOrJobFilename = "bqtest.comet.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.comet.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          cleanMetadata
          cleanDatasets

          logger.info(settings.comet.datasets)
          secure(WatchConfig())
        }
      }
      val tableFound =
        Option(bigquery.getTable(TableId.of("bqtest", "account"))).isDefined
      tableFound should be(true)

    }
  }

  "Native BigQuery AutoJob" should "succeed" in {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      new WithSettings() {
        new SpecTrait(
          domainOrJobFilename = "bqtest.comet.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.comet.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          val businessTask1 = AutoTaskDesc(
            "",
            Some("select * except(code0000) from bqtest.account"),
            None,
            "bqtest",
            "jobresult",
            WriteMode.OVERWRITE,
            sink = Some(BigQuerySink(name = Some("sinktest"), location = Some("EU"))),
            engine = Some(Engine.BQ),
            python = None,
            merge = None
          )
          val businessJob =
            AutoJobDesc(
              "bqjobtest",
              List(businessTask1),
              Nil,
              None,
              None,
              None,
              engine = Some(Engine.BQ)
            )

          val businessJobDef = mapper
            .writer()
            .withAttribute(classOf[Settings], settings)
            .writeValueAsString(businessJob)
          val pathBusiness = new Path(cometMetadataPath + "/jobs/bqjobtest.comet.yml")
          storageHandler.write(businessJobDef, pathBusiness)

          val schemaHandler = new SchemaHandler(metadataStorageHandler)

          val workflow =
            new IngestionWorkflow(storageHandler, schemaHandler, new SimpleLauncher())
          val config = TransformConfig("bqjobtest")
          workflow.autoJob(config) should be(true)
          workflow.autoJob(config.copy(interactive = Some("json"))) should be(true)
          workflow.autoJob(config.copy(interactive = Some("csv"))) should be(true)
          workflow.autoJob(config.copy(interactive = Some("table"))) should be(true)
        }
      }
    }
  }
  "Extract Table infos" should "succeed" in {
    new WithSettings() {
      val logTime = java.sql.Timestamp.from(Instant.now)
      val start = System.currentTimeMillis()
      val infos = BigQueryInfo.extractInfo(BigQueryConnectionConfig())
      val end = System.currentTimeMillis()
      println((end - start) / 1000)
      val datasetInfos = infos.map(_._1).map(BigQueryDatasetInfo(_, logTime))
      val tableInfos = infos.flatMap(_._2).map(BigQueryTableInfo(_, logTime))
      println(JsonSerializer.serializeObject(datasetInfos))
      println(JsonSerializer.serializeObject(tableInfos))
      val config = BigQueryTablesConfig()
      BigQueryTableInfo.sink(config)
    }
  }
  "Freshness of Table" should "return list of warning & errors" in {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      import org.slf4j.impl.StaticLoggerBinder
      val binder = StaticLoggerBinder.getSingleton
      logger.debug(binder.getLoggerFactory.toString)
      logger.debug(binder.getLoggerFactoryClassStr)

      new WithSettings() {
        new SpecTrait(
          domainOrJobFilename = "bqtest.comet.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.comet.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          val config = BigQueryFreshnessConfig(tables = Map("bqtest" -> List("account")))
          val result = BigQueryFreshnessInfo.freshness(config)
          val json = JsonSerializer.serializeObject(result)
          println(json)

        }
      }
    }
  }
}
