package com.ebiznext.comet.job

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extractor.ScriptGen
import ai.starlake.job.atlas.AtlasConfig
import ai.starlake.job.convert.{Parquet2CSV, Parquet2CSVConfig}
import ai.starlake.job.sink.bigquery.BigQueryLoadConfig
import ai.starlake.job.sink.jdbc.ConnectionLoadConfig
import ai.starlake.job.sink.es.ESLoadConfig
import ai.starlake.job.sink.kafka.KafkaJobConfig
import ai.starlake.job.infer.InferSchemaConfig
import ai.starlake.job.ingest.LoadConfig
import ai.starlake.job.metrics.MetricsConfig
import ai.starlake.schema.generator._
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.CometObjectMapper
import ai.starlake.workflow.{ImportConfig, IngestionWorkflow, TransformConfig, WatchConfig}
import buildinfo.BuildInfo
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import scala.util.{Failure, Success, Try}

/** The root of all things.
  *   - importing from landing
  *   - submitting requests to the cron manager
  *   - ingesting the datasets
  *   - running an auto job All these things are launched from here. See printUsage below to
  *     understand the CLI syntax.
  */
object Main extends StrictLogging {
  // uses Jackson YAML to parsing, relies on SnakeYAML for low level handling
  val mapper: ObjectMapper = new CometObjectMapper(new YAMLFactory)
  mapper.setSerializationInclusion(Include.NON_EMPTY)

  private def printUsage() = {
    // scalastyle:off println
    println(
      s"""
         |Usage : One of
         |${LoadConfig.usage()}
         |${ImportConfig.usage()}
         |${TransformConfig.usage()}
         |${WatchConfig.usage()}
         |${ESLoadConfig.usage()}
         |${BigQueryLoadConfig.usage()}
         |${InferSchemaConfig.usage()}
         |${MetricsConfig.usage()}
         |${Parquet2CSVConfig.usage()}
         |${Xls2YmlConfig.usage()}
         |${Yml2XlsConfig.usage()}
         |${KafkaJobConfig.usage()}
         |${Yml2GraphVizConfig.usage()}
         |""".stripMargin
    )
    // scalastyle:on println
  }

  /** @param args
    *   depends on the action required to run a job:
    *   - call "comet job jobname" where jobname is the name of the job as defined in one of the
    *     definition files present in the metadata/jobs folder. to import files from a local file
    *     system
    *   - call "comet import", this will move files in the landing area to the pending area to watch
    *     for files wiating to be processed
    *   - call"comet watch [{+|–}domain1,domain2,domain3]" with a optional domain list separated by
    *     a ','. When called without any domain, will watch for all domain folders in the landing
    *     area When called with a '+' sign, will look only for this domain folders in the landing
    *     area When called with a '-' sign, will look for all domain folder in the landing area
    *     except the ones in the command lines.
    *   - call "comet ingest domain schema hdfs://datasets/domain/pending/file.dsv" to ingest a file
    *     defined by its schema in the specified domain
    * -call "comet infer-schema --domain domainName --schema schemaName --input datasetpath --output
    * outputPath --with-header boolean
    *   - call "comet metrics --domain domain-name --schema schema-name " to compute all metrics on
    *     specific schema in a specific domain
    */
  def main(args: Array[String]): Unit = {
    logger.warn(
      "com.ebiznext.comet.job.Main is deprecated. Please start using ai.starlake.job.Main"
    )
    Thread.sleep(10 * 1000)
    legacyMain(args)
  }

  def legacyMain(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    logger.info(s"Comet Version ${BuildInfo.version}")
    import settings.{launcherService, metadataStorageHandler, storageHandler}
    DatasetArea.initMetadata(metadataStorageHandler)
    val schemaHandler = new SchemaHandler(metadataStorageHandler)
    Try {
      schemaHandler.checkValidity()
    } match {
      case Success(_) => // do nothing
      case Failure(e) =>
        e.printStackTrace()
        if (settings.comet.validateOnLoad)
          throw e
    }

    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val workflow =
      new IngestionWorkflow(storageHandler, schemaHandler, launcherService)

    if (args.length == 0) printUsage()

    val arglist = args.toList
    logger.info(s"Running Starlake $arglist")
    val result = arglist.head match {
      case "job" | "transform" =>
        TransformConfig.parse(args.drop(1)) match {
          case Some(config) =>
            // do something
            workflow.autoJob(config)
          case _ =>
            println(TransformConfig.usage())
            false
        }
      case "import" =>
        ImportConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.loadLanding(config)
            true
          case None =>
            println(ImportConfig.usage())
            false
        }
      case "validate" =>
        schemaHandler.checkValidity()
        true
      case "watch" =>
        WatchConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.loadPending(config)
          case _ =>
            println(WatchConfig.usage())
            false
        }
      case "ingest" | "load" =>
        LoadConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.load(config)
          case _ =>
            println(LoadConfig.usage())
            false
        }

      case "index" | "esload" =>
        ESLoadConfig.parse(args.drop(1)) match {
          case Some(config) =>
            // do something
            workflow.esLoad(config).isSuccess
          case _ =>
            println(ESLoadConfig.usage())
            false
        }

      case "atlas" =>
        AtlasConfig.parse(args.drop(1)) match {
          case Some(config) =>
            // do something
            workflow.atlas(config)
          case _ =>
            println(AtlasConfig.usage())
            false
        }

      case "kafkaload" =>
        KafkaJobConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.kafkaload(config).isSuccess
          case _ =>
            println(KafkaJobConfig.usage())
            false
        }

      case "bqload" =>
        BigQueryLoadConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.bqload(config).isSuccess
          case _ =>
            println(BigQueryLoadConfig.usage())
            false
        }

      case "cnxload" =>
        ConnectionLoadConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.jdbcload(config).isSuccess
          case _ =>
            println(ConnectionLoadConfig.usage())
            false
        }

      case "infer-ddl" =>
        Yml2DDLConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.inferDDL(config).isSuccess
          case _ =>
            println(Yml2DDLConfig.usage())
            false
        }
      case "infer-schema" =>
        InferSchemaConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.inferSchema(config).isSuccess
          case _ =>
            println(InferSchemaConfig.usage())
            false
        }
      case "metrics" =>
        MetricsConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.metric(config).isSuccess
          case _ =>
            println(MetricsConfig.usage())
            false
        }

      case "parquet2csv" =>
        Parquet2CSVConfig.parse(args.drop(1)) match {
          case Some(config) =>
            new Parquet2CSV(config, storageHandler).run().isSuccess
          case _ =>
            println(Parquet2CSVConfig.usage())
            false
        }

      case "secure" =>
        WatchConfig.parse(args.drop(1)) match {
          case Some(config) =>
            workflow.secure(config)
          case _ =>
            println(WatchConfig.usage())
            false
        }

      case "xls2yml" =>
        Xls2Yml.run(args.drop(1))

      case "yml2xls" =>
        new Yml2XlsWriter(schemaHandler).run(args.drop(1))
        true

      case "yml2gv" =>
        new Yml2GraphViz(schemaHandler).run(args.drop(1))
        true

      case "extract" =>
        new ScriptGen(storageHandler, schemaHandler, launcherService).run(args.drop(1))
      case _ =>
        printUsage()
        false
    }
    if (!result)
      throw new Exception(s"""Comet failed to execute command with args ${args.mkString(",")}""")
  }
}
