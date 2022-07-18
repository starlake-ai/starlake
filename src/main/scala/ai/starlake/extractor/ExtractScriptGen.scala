package ai.starlake.extractor

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extractor.config.{Settings => ExtractorSettings}
import ai.starlake.schema.handlers.{LaunchHandler, SchemaHandler, SimpleLauncher, StorageHandler}
import ai.starlake.schema.model.Engine.{BQ, SPARK}
import ai.starlake.schema.model.{AutoJobDesc, Domain}
import ai.starlake.utils.Formatter._
import ai.starlake.workflow.IngestionWorkflow
import better.files.File
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.fusesource.scalate._

class ScriptGen(
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  launchHandler: LaunchHandler
)(implicit settings: Settings)
    extends StrictLogging {
  val engine: TemplateEngine = new TemplateEngine

  /** Generate an extraction script payload based on a template and its params
    * @param template
    *   The extraction script template
    * @param templateParams
    *   Its params
    * @return
    *   The produced script payload
    */
  def templatize(template: File, templateParams: TemplateParams): List[File] = {
    def formatFilename(name: String): String =
      name
        .substring(0, name.lastIndexOf("."))
        .replaceAll("domain", templateParams.domainToExport)
        .replaceAll("schema", templateParams.tableToExport)

    val filesPath = if (template.isDirectory) {
      template.list
        .map(_.pathAsString)
        .filter(name => name.endsWith(".ssp") || name.endsWith(".mustache"))
        .map(name => (name, formatFilename(name)))

    } else {
      val outputFilename =
        templateParams.scriptOutputFile
          .map(_.pathAsString)
          .getOrElse(formatFilename(template.pathAsString))
      Iterator((template.pathAsString, outputFilename))
    }

    filesPath.map { case (inputPath, outputPath) =>
      val scriptPayload = engine.layout(
        inputPath,
        templateParams.paramMap
      )
      val outputFile = File(outputPath)
      outputFile.createFileIfNotExists().overwrite(scriptPayload)

      logger.info(s"Successfully generated script $outputFile")
      outputFile
    }.toList
  }

  /** Generate all extraction scripts based on the given domain
    * @param domain
    *   The domain extracted from the Excel referential file
    * @param scriptTemplateFile
    *   The script template
    * @param scriptsOutputPath
    *   Where the scripts are produced
    * @param defaultDeltaColumn
    *   Defaut delta column
    * @param deltaColumns
    *   Mapping table name -> delta column, has precedence over `defaultDeltaColumn`
    * @return
    *   The list of produced files
    */
  def generateDomain(
    domain: Domain,
    scriptTemplateFile: File,
    scriptsOutputPath: File,
    scriptOutputPattern: Option[String],
    defaultDeltaColumn: Option[String],
    deltaColumns: Map[String, String],
    activeEnv: Map[String, String]
  ): List[File] = {
    val templateSettings =
      TemplateParams.fromDomain(
        domain,
        scriptsOutputPath,
        scriptOutputPattern,
        defaultDeltaColumn,
        deltaColumns,
        activeEnv
      )
    templateSettings.flatMap { ts =>
      templatize(scriptTemplateFile, ts)
    }
  }

  /** Generate all extraction scripts based on the given domain
    * @param job
    *   The job extracted from the yml file
    * @param scriptTemplateFile
    *   The script template
    * @param scriptsOutputFile
    *   Where the scripts are produced
    * @return
    *   The list of produced files
    */
  def generateJob(
    job: AutoJobDesc,
    scriptTemplateFile: File,
    scriptsOutputFolder: File,
    scriptOutputPattern: Option[String]
  ): File = {
    import settings.metadataStorageHandler
    val workflow =
      new IngestionWorkflow(metadataStorageHandler, schemaHandler, new SimpleLauncher())
    val actions = workflow.buildTasks(job.name, Map.empty[String, String], None)
    actions.map { action =>
      val (preSql, sql, postSql) = action.engine match {
        case BQ =>
          action.buildQueryBQ()
        case SPARK =>
          action.buildQuerySpark()
        case _ =>
          throw new Exception("not supported") // TODO
      }
      action.copy(task =
        action.task.copy(presql = Some(preSql), sql = Option(sql), postsql = Some(postSql))
      )(settings, metadataStorageHandler, schemaHandler)
    }

    val scriptPayload = engine.layout(
      scriptTemplateFile.pathAsString,
      Map(
        "job"     -> job,
        "actions" -> actions,
        "env"     -> schemaHandler.activeEnv
      )
    )
    val scriptOutputFileName = scriptOutputPattern
      .map(
        _.richFormat(
          schemaHandler.activeEnv,
          Map(
            "job" -> job.name
          )
        )
      )
      .getOrElse(s"${job.name}.py")
    val scriptOutputFile = scriptsOutputFolder / scriptOutputFileName
    val scriptFile =
      scriptOutputFile.createFileIfNotExists().overwrite(scriptPayload)
    logger.info(s"Successfully generated job script $scriptFile")
    scriptFile
  }

  /** Fills a Mustache templated file based on a given domain. The following documentation considers
    * that we use the script to generate SQL export files.
    *
    * The schemas should at least, specify :
    *   - a table name (schemas.name)
    *   - a file pattern (schemas.pattern) which is used as the export file base name
    *   - a write mode (schemas.metadata.write): APPEND or OVERWRITE
    *   - the columns to extract (schemas.attributes.name*)
    *
    * You also have to provide a Mustache (http://mustache.github.io/mustache.5.html) template file.
    *
    * Here you'll write your extraction export process (sqlplus for Oracle, pgsql for PostgreSQL as
    * an example). In that template you can use the following parameters:
    *
    * table_name -> the table to export delimiter -> the resulting dsv file delimiter columns -> the
    * columns to export columns is a Mustache map, it gives you access, for each column, to:
    *   - name -> the column name
    *   - trailing_col_char -> the separator to append to the column (, if there are more columns to
    *     come, "" otherwise) Here is an example how to use it in a template: SELECT {{#columns}}
    *     TO_CHAR({{name}}){{trailing_col_char}} {{/columns}} FROM {{table_name}}; export_file ->
    *     the export file name delta_column -> a delta date column (passed as a Main arg or as a
    *     config element), the column which is used to determine new rows for each exports in APPEND
    *     mode full_export -> if the export is a full or delta export (the logic is to be
    *     implemented in your script)
    *
    * Usage: starlake [script-gen] [options]
    *
    * Command: script-gen
    * --domain <value> The domain for which to generate extract scripts
    * --templateFile <value> Script template file
    * --scriptsOutputDir <value> Scripts output folder
    * --deltaColumn <value> The date column which is used to determine new rows for each exports
    * (can be passed table by table as config element)
    */
  def run(args: Array[String]): Boolean = {

    val arglist = args.toList
    logger.info(s"Running Starlake $arglist")

    ExtractScriptGenConfig.parse(args) match {
      case Some(config) =>
        run(config)
      case _ =>
        logger.error("Program execution or parameters are wrong, please check usage")
        false
    }
  }

  def run(config: ExtractScriptGenConfig)(implicit settings: Settings): Boolean = {
    import settings.metadataStorageHandler
    DatasetArea.initMetadata(metadataStorageHandler)
    val schemaHandler = new SchemaHandler(metadataStorageHandler)
    (config.domain, config.jobs) match {
      case (Nil, Nil) =>
        logger.warn(s"No domain or jobs provided. Extracting all domains")
        val domainNames = schemaHandler.domains.map(_.name)
        runOnDomains(config, schemaHandler, domainNames)
      case (Nil, jobNames) =>
        runOnJobs(config, schemaHandler, jobNames)
      case (domainNames, Nil) =>
        runOnDomains(config, schemaHandler, domainNames)
      case (_, _) =>
        logger.error(s"Only one of domain or job list should be passed as an argument")
        false
    }
  }

  private def runOnDomains(
    config: ExtractScriptGenConfig,
    schemaHandler: SchemaHandler,
    domainNames: Seq[String]
  ): Boolean = {
    val domains: List[Domain] = schemaHandler.domains
    domainNames
      .map { domainName =>
        // Extracting the domain from the Excel referential file
        domains.find(_.name == domainName) match {
          case Some(domain) =>
            generateDomain(
              domain,
              config.scriptTemplateFile,
              config.scriptOutputDir,
              config.scriptOutputPattern,
              config.deltaColumn.orElse(ExtractorSettings.deltaColumns.defaultColumn),
              ExtractorSettings.deltaColumns.deltaColumns,
              schemaHandler.activeEnv
            )
            true
          case None =>
            logger.error(s"No domain found for domain name ${config.domain}")
            false
        }
      }
      .forall(_ == true)
  }

  private def runOnJobs(
    config: ExtractScriptGenConfig,
    schemaHandler: SchemaHandler,
    jobNames: Seq[String]
  ): Boolean = {
    val jobs = schemaHandler.jobs
    jobNames
      .map { jobName =>
        // Extracting the Job
        jobs.get(jobName) match {
          case Some(job) =>
            generateJob(
              job,
              config.scriptTemplateFile,
              config.scriptOutputDir,
              config.scriptOutputPattern
            )
            true
          case None =>
            logger.error(s"No file found for domain name ${config.domain}")
            false
        }
      }
      .forall(_ == true)
  }
}

object Main {
  implicit val settings: Settings = Settings(ConfigFactory.load())
  import settings.{launcherService, metadataStorageHandler, storageHandler}
  DatasetArea.initMetadata(metadataStorageHandler)
  val schemaHandler = new SchemaHandler(metadataStorageHandler)

  def main(args: Array[String]): Unit = {
    val result = new ScriptGen(storageHandler, schemaHandler, launcherService).run(args)
    if (!result) throw new Exception("ScriptGen failed!")
  }
}
