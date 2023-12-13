package ai.starlake.job

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract._
import ai.starlake.job.bootstrap.BootstrapCmd
import ai.starlake.job.convert.Parquet2CSVCmd
import ai.starlake.job.infer.InferSchemaCmd
import ai.starlake.job.ingest.{IamPoliciesCmd, ImportCmd, IngestCmd, LoadCmd, SecureCmd}
import ai.starlake.job.metrics.MetricsCmd
import ai.starlake.job.sink.es.ESLoadCmd
import ai.starlake.job.sink.jdbc.JdbcConnectionLoadCmd
import ai.starlake.job.sink.kafka.KafkaJobCmd
import ai.starlake.job.site.SiteCmd
import ai.starlake.job.transform.TransformCmd
import ai.starlake.schema.generator._
import ai.starlake.schema.handlers.{SchemaHandler, ValidateCmd}
import ai.starlake.schema.ProjectCompareCmd
import ai.starlake.serve.MainServerCmd
import ai.starlake.utils._
import buildinfo.BuildInfo
import com.typesafe.scalalogging.StrictLogging

import java.io.ByteArrayOutputStream
import scala.annotation.nowarn
import scala.util.{Failure, Success, Try}

/** The root of all things.
  *   - importing from landing
  *   - submitting requests to the cron manager
  *   - ingesting the datasets
  *   - running an auto job All these things are launched from here. See printUsage below to
  *     understand the CLI syntax.
  */

object Main extends StrictLogging {

  final val shell: String = "starlake"

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
  @nowarn
  def main(args: Array[String]): Unit = {
    ProxySettings.setProxy()
    val settings: Settings = Settings(Settings.referenceConfig)
    logger.debug(settings.toString)
    new Main().run(args)(settings)
  }

}

class Main() extends StrictLogging {

  val commands: List[Cmd[_]] = List(
    BootstrapCmd,
    TransformCmd,
    ImportCmd,
    ValidateCmd,
    LoadCmd,
    IngestCmd,
    ESLoadCmd,
    KafkaJobCmd,
    JdbcConnectionLoadCmd,
    Yml2DDLCmd,
    InferSchemaCmd,
    MetricsCmd,
    Parquet2CSVCmd,
    SiteCmd,
    SecureCmd,
    IamPoliciesCmd,
    Xls2YmlCmd,
    Yml2XlsCmd,
    Xls2YmlAutoJobCmd,
    TableDependenciesCmd,
    AclDependenciesCmd,
    AutoTaskDependenciesCmd,
    ExtractCmd,
    ExtractJDBCSchemaCmd,
    ExtractDataCmd,
    ExtractScriptCmd,
    BigQueryTableInfoCmd,
    ExtractBigQuerySchemaCmd,
    BigQueryFreshnessInfoCmd,
    ProjectCompareCmd,
    MainServerCmd,
    DagGenerateCmd
  )
  private def printUsage(): Unit = {
    // scalastyle:off println
    println(s"Starlake Version ${BuildInfo.version}")
    println("Usage:")
    println(s"\t${Main.shell} [command]")
    println("Available commands =>")
    commands.foreach { cmd =>
      println(s"\t${cmd.command}")
    }
  }
  private def printUsage(command: String): Unit = {
    // scalastyle:off println
    commands.find(_.command == command) match {
      case None =>
        println(s"ERROR: Unknown command --> $command")
      case Some(cmd) =>
        println(cmd.usage())
    }
  }
  // scalastyle:on println

  def checkPrerequisites(args: List[String]): Unit = {
    args match {
      case Nil | "help" :: Nil =>
        printUsage()
        System.exit(0)
      case "help" :: command :: _ =>
        printUsage(command)
        System.exit(0)
      case _ =>
    }

    sys.env.get("SL_ROOT") match {
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

    import settings.storageHandler
    DatasetArea.initMetadata(storageHandler())

    // extract any env var passed as --options argument
    val cliEnv = CliEnvConfig.parse(args.drop(1)) match {
      case Some(env) => env.options
      case None      => Map.empty[String, String]
    }

    val schemaHandler = new SchemaHandler(storageHandler(), cliEnv)
    val executedCommand = argList.mkString(" ")
    logger.info(s"Running Starlake $executedCommand")
    val errCapture = new ByteArrayOutputStream()
    Console.withErr(errCapture) {
      val result = run(args, schemaHandler)
      // We raise an exception only on command failure not on parse args failure
      result match {
        case Failure(e: IllegalArgumentException) =>
          // scalastyle:off println
          val errMessage = s"""
             |--------------------------------------------------
             |${errCapture.toString().trim}
             |--------------------------------------------------
             |${e.getMessage}""".stripMargin

          System.err.print(errMessage)
          if (settings.appConfig.forceHalt) {
            Runtime.getRuntime.halt(1)
          }

        case Failure(exception) =>
          val message = s"""Starlake failed to execute command with args $executedCommand"""
          System.err.print(message)
          Utils.logException(logger, exception)
          if (settings.appConfig.forceHalt) {
            Runtime.getRuntime.halt(1)
          } else {
            throw exception
          }
        case Success(_) =>
          logger.info(s"Successfully $executedCommand")
          if (settings.appConfig.forceHalt) {
            Runtime.getRuntime.halt(0)
          }
      }
    }

  }

  def run(args: Array[String], schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[Any] = {

    val command = args.headOption.getOrElse("")

    val result =
      commands.find(_.command == command) match {
        case None =>
          printUsage(command)
          Failure(new IllegalArgumentException(s"Unknown command $command"))
        case Some(cmd) =>
          if (cmd.command != BootstrapCmd.command && settings.appConfig.validateOnLoad)
            schemaHandler.checkValidity()
          val r = cmd.run(args.drop(1), schemaHandler)
          if (cmd.command == BootstrapCmd.command)
            System.exit(0)
          r
      }

    result
  }
}
