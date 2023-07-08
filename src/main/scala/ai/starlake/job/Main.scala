package ai.starlake.job

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract._
import ai.starlake.job.atlas.AtlasConfig
import ai.starlake.job.bootstrap.BootstrapConfig
import ai.starlake.job.convert.{FileSplitterConfig, Parquet2CSV, Parquet2CSVConfig}
import ai.starlake.job.infer.InferSchemaConfig
import ai.starlake.job.ingest.{ImportConfig, LoadConfig, WatchConfig}
import ai.starlake.job.metrics.MetricsConfig
import ai.starlake.job.sink.bigquery.BigQueryLoadConfig
import ai.starlake.job.sink.es.ESLoadConfig
import ai.starlake.job.sink.jdbc.ConnectionLoadConfig
import ai.starlake.job.sink.kafka.KafkaJobConfig
import ai.starlake.job.transform.{AutoTask2GraphVizConfig, AutoTaskToGraphViz, TransformConfig}
import ai.starlake.schema.generator._
import ai.starlake.schema.generator.yml2dag.config.Yml2DagConfigForMain
import ai.starlake.schema.handlers.{SchemaHandler, ValidateConfig}
import ai.starlake.schema.{ProjectCompare, ProjectCompareConfig}
import ai.starlake.serve.{MainServerConfig, SingleUserMainServer}
import ai.starlake.utils.{CliConfig, CliEnvConfig, DeprecatedChecks, JsonSerializer}
import ai.starlake.workflow.IngestionWorkflow
import buildinfo.BuildInfo
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

/** The root of all things.
  *   - importing from landing
  *   - submitting requests to the cron manager
  *   - ingesting the datasets
  *   - running an auto job All these things are launched from here. See printUsage below to
  *     understand the CLI syntax.
  */

object Main extends StrictLogging {

  /** @param args
    *   depends on the action required to run a job:
    *   - call "starlake transform jobname" where jobname is the name of the job as defined in one
    *     of the definition files present in the metadata/jobs folder. to import files from a local
    *     file system
    *   - call "starlake import", this will move files in the landing area to the pending area to
    *     watch for files waiting to be processed
    *   - call"starlake watch [{+|–}domain1,domain2,domain3]" with a optional domain list separated
    *     by a ','. When called without any domain, will watch for all domain folders in the landing
    *     area When called with a '+' sign, will look only for this domain folders in the landing
    *     area When called with a '-' sign, will look for all domain folder in the landing area
    *     except the ones in the command lines.
    *   - call "starlake ingest domain schema hdfs://datasets/domain/pending/file.dsv" to ingest a
    *     file defined by its schema in the specified domain
    * -call "starlake infer-schema --domain domainName --schema schemaName --input datasetpath
    * --output outputPath --with-header
    *   - call "starlake metrics --domain domain-name --schema schema-name " to compute all metrics
    *     on specific schema in a specific domain
    */
  def main(args: Array[String]): Unit = {
    DeprecatedChecks.cometEnvVars()
    val settings: Settings = Settings(ConfigFactory.load())
    logger.info(settings.toString)
    new Main().run(args)(settings)
  }

}

class Main() extends StrictLogging {

  val configs: List[CliConfig[_]] = List(
    AutoTask2GraphVizConfig,
    BootstrapConfig,
    BigQueryLoadConfig,
    BigQueryTablesConfig,
    ProjectCompareConfig,
    ConnectionLoadConfig,
    ESLoadConfig,
    ExtractDataConfig,
    ExtractSchemaConfig,
    ExtractScriptConfig,
    FileSplitterConfig,
    ImportConfig,
    InferSchemaConfig,
    KafkaJobConfig,
    LoadConfig,
    MetricsConfig,
    Parquet2CSVConfig,
    TransformConfig,
    WatchConfig,
    Xls2YmlConfig,
    Yml2DDLConfig,
    Yml2GraphVizConfig,
    Yml2XlsConfig,
    Yml2DagConfigForMain
  )
  private def printUsage() = {
    // scalastyle:off println
    println(s"Starlake Version ${BuildInfo.version}")
    println("Usage:")
    println("\tstarlake [command]")
    println("Available commands =>")
    configs.foreach { config =>
      println(s"\t${config.command}")
    }
  }
  private def printUsage(command: String) = {
    // scalastyle:off println
    configs.find(_.command == command) match {
      case None =>
        println(s"ERROR: Unknown command --> $command")
      case Some(config) =>
        println(config.usage())
    }
  }
  // scalastyle:on println

  def checkPrerequisites(args: List[String]) = {
    args match {
      case Nil | "help" :: Nil =>
        printUsage()
        System.exit(0)
      case "help" :: command :: any =>
        printUsage(command)
        System.exit(0)
      case _ =>
    }

    sys.env.get("COMET_ROOT").orElse(sys.env.get("SL_ROOT")) match {
      case None =>
        logger.warn(
          "Define and set the SL_ROOT env variable to your starlake project folder"
        )
      case Some(rootDir) =>
        logger.info(s"Project located in $rootDir")
    }

  }

  def run(args: Array[String])(implicit settings: Settings): Unit = {
    logger.info(s"Starlake Version ${BuildInfo.version}")
    val argList = args.toList
    checkPrerequisites(argList)

    import settings.{launcherService, storageHandler}
    DatasetArea.initMetadata(storageHandler)

    // extract any env var passed as --options argument
    val cliEnv = CliEnvConfig.parse(args.drop(1)) match {
      case Some(env) => env.options
      case None      => Map.empty[String, String]
    }

    val schemaHandler = new SchemaHandler(storageHandler, cliEnv)

    // handle non existing project commands
    argList.head match {
      case "bootstrap" =>
        BootstrapConfig.parse(args.drop(1)) match {
          case Some(config) =>
            DatasetArea.bootstrap(config.template)
          case None =>
            println(BootstrapConfig.usage())
            false

        }
        System.exit(0)
      case _ =>
    }

    if (settings.comet.validateOnLoad)
      schemaHandler.fullValidation()

    DatasetArea.initDomains(storageHandler, schemaHandler.domains().map(_.name))
    val workflow =
      new IngestionWorkflow(storageHandler, schemaHandler, launcherService)

    logger.info(s"Running Starlake $argList")
    val result = argList.head match {
      case "job" | "transform" =>
        TransformConfig.parse(args.drop(1)) match {
          case Some(config) =>
            if (config.compile) {
              workflow.compileAutoJob(config)
              true
            } else
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
        ValidateConfig.parse(args.drop(1)) match {
          case Some(config) =>
            schemaHandler.fullValidation(config)
            true
          case _ =>
            println(WatchConfig.usage())
            false
        }
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
            workflow.bqload(config.asBigqueryLoadConfig()).isSuccess
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

      case "iam-policies" =>
        workflow.applyIamPolicies()
        true

      case "xls2yml" =>
        Xls2Yml.run(args.drop(1))

      case "yml2xls" =>
        new Yml2Xls(schemaHandler).run(args.drop(1))
        true

      case "xls2ymljob" =>
        Xls2YmlAutoJob.run(args.drop(1))

      case "yml2gv" =>
        new Yml2GraphViz(schemaHandler).run(args.drop(1))
        true
      case "jobs2gv" =>
        AutoTask2GraphVizConfig.parse(args.drop(1)) match {
          case Some(config) =>
            new AutoTaskToGraphViz(settings, schemaHandler, storageHandler).run(config)
          case None =>
            println(AutoTask2GraphVizConfig.usage())
        }
        true
      case "extract-script" =>
        new ExtractScript(schemaHandler).run(args.drop(1))
      case "extract-schema" =>
        new ExtractJDBCSchema(schemaHandler).run(args.drop(1))
        true
      case "extract-data" =>
        new ExtractData(schemaHandler).run(args.drop(1))
        true
      case "bq-info" =>
        BigQueryTableInfo.run(args.drop(1))
        true
      case "extract-bq-schema" =>
        ExtractBigQuerySchema.run(args.drop(1))
        true
      case "bq-freshness" =>
        val result = BigQueryFreshnessInfo.run(args.drop(1))
        val warnFound = result.find(_.warnOrError == "WARN")
        val errFound = result.find(_.warnOrError == "ERROR")
        // scalastyle:off println
        println(JsonSerializer.serializeObject(result))
        if (errFound.isDefined)
          System.exit(2)
        if (warnFound.isDefined)
          System.exit(1)
        true
      case "compare" =>
        ProjectCompare.run(args.drop(1))
        true
      case "serve" =>
        MainServerConfig.parse(args.drop(1)) match {
          case Some(config) =>
            SingleUserMainServer.serve(config)
            true
          case _ =>
            println(MainServerConfig.usage())
            false
        }
      case Yml2DagCommandDispatcher.name =>
        new Yml2DagCommandDispatcher(schemaHandler).run(args.tail)
        true
      case command =>
        printUsage(command)
        false
    }
    if (!result)
      throw new Exception(s"""Comet failed to execute command with args ${args.mkString(",")}""")
  }
}
