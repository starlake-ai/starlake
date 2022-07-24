package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, SimpleLauncher}
import ai.starlake.schema.model._
import ai.starlake.workflow.{IngestionWorkflow, TransformConfig, WatchConfig}
import com.google.cloud.bigquery.TableId
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

class BigQueryNativeJobSpec extends TestHelper with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    if (sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
      BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "account"))
      BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }
  override def afterAll(): Unit = {
    if (sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "account"))
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }

  "Ingest to BigQuery" should "be ingest and store table in BigQuery" in {
    if (sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
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
        Option(BigQueryJobBase.bigquery.getTable(TableId.of("bqtest", "account"))).isDefined
      tableFound should be(true)

    }
  }
  "Secure BigQuery Tables" should "should set policies in tables" in {
    if (false && sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
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
        Option(BigQueryJobBase.bigquery.getTable(TableId.of("bqtest", "account"))).isDefined
      tableFound should be(true)

    }
  }

  "Native BigQuery AutoJob" should "succeed" in {
    if (sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
      new WithSettings() {
        new SpecTrait(
          domainOrJobFilename = "bqtest.comet.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.comet.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          val businessTask1 = AutoTaskDesc(
            None,
            Some("select * except(code0) from bqtest.account"),
            "bqtest",
            "jobresult",
            WriteMode.OVERWRITE,
            sink = Some(BigQuerySink(name = Some("sinktest"), location = Some("EU"))),
            engine = Some(Engine.BQ)
          )
          val businessJob =
            AutoJobDesc(
              "bqjobtest",
              List(businessTask1),
              None,
              None,
              None,
              engine = Some(Engine.BQ)
            )
          val schemaHandler = new SchemaHandler(metadataStorageHandler)

          val businessJobDef = mapper
            .writer()
            .withAttribute(classOf[Settings], settings)
            .writeValueAsString(businessJob)
          val pathBusiness = new Path(cometMetadataPath + "/jobs/bqjobtest.comet.yml")
          storageHandler.write(businessJobDef, pathBusiness)

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
}
