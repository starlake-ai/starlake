package com.ebiznext.comet.extractor

import better.files.File
import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.extractor.config.{Settings => ExtractorSettings}
import com.ebiznext.comet.schema.generator.YamlSerializer
import com.ebiznext.comet.schema.handlers.SchemaHandler
import com.ebiznext.comet.schema.model.{AutoJobDesc, Domain}
import com.ebiznext.comet.utils.Formatter.RichFormatter
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.fusesource.scalate._

object ScriptGen extends StrictLogging {

  implicit val settings: Settings = Settings(ConfigFactory.load())
  val engine: TemplateEngine = new TemplateEngine

  /** Generate an extraction script payload based on a template and its params
    * @param template The extraction script template
    * @param templateParams Its params
    * @return The produced script payload
    */
  def templatize(template: File, templateParams: TemplateParams): String =
    engine.layout(
      template.pathAsString,
      templateParams.paramMap
    )

  /** Generate all extraction scripts based on the given domain
    * @param domain The domain extracted from the Excel referential file
    * @param scriptTemplateFile The script template
    * @param scriptsOutputPath Where the scripts are produced
    * @param defaultDeltaColumn Defaut delta column
    * @param deltaColumns Mapping table name -> delta column, has precedence over `defaultDeltaColumn`
    * @return The list of produced files
    */
  def generateDomain(
    domain: Domain,
    scriptTemplateFile: File,
    scriptsOutputPath: File,
    scriptOutputPattern: Option[String],
    defaultDeltaColumn: Option[String],
    deltaColumns: Map[String, String]
  ): List[File] = {
    val templateSettings =
      TemplateParams.fromDomain(
        domain,
        scriptsOutputPath,
        scriptOutputPattern,
        defaultDeltaColumn,
        deltaColumns
      )
    templateSettings.map { ts =>
      val scriptPayload = templatize(scriptTemplateFile, ts)
      val scriptFile =
        ts.scriptOutputFile.createFileIfNotExists().overwrite(scriptPayload)
      logger.info(s"Successfully generated script $scriptFile")
      scriptFile
    }
  }

  /** Generate all extraction scripts based on the given domain
    * @param job The job extracted from the yml file
    * @param scriptTemplateFile The script template
    * @param scriptsOutputFile Where the scripts are produced
    * @return The list of produced files
    */
  def generateJob(
    job: AutoJobDesc,
    scriptTemplateFile: File,
    scriptsOutputFolder: File,
    scriptOutputPattern: Option[String]
  ): File = {

    val scriptPayload = engine.layout(
      scriptTemplateFile.pathAsString,
      YamlSerializer.toMap(job)
    )
    val scriptOutputFileName = scriptOutputPattern
      .map(
        _.richFormat(
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

  /** Fills a Mustache templated file based on a given domain.
    * The following documentation considers that we use the script to generate SQL export files.
    *
    * The schemas should at least, specify :
    *    - a table name (schemas.name)
    *    - a file pattern (schemas.pattern) which is used as the export file base name
    *    - a write mode (schemas.metadata.write): APPEND or OVERWRITE
    *    - the columns to extract (schemas.attributes.name*)
    *
    * You also have to provide a Mustache (http://mustache.github.io/mustache.5.html) template file.
    *
    * Here you'll write your extraction export process (sqlplus for Oracle, pgsql for PostgreSQL as an example).
    * In that template you can use the following parameters:
    *
    * table_name  -> the table to export
    * delimiter   -> the resulting dsv file delimiter
    * columns     -> the columns to export
    * columns is a Mustache map, it gives you access, for each column, to:
    *  - name               -> the column name
    *  - trailing_col_char  -> the separator to append to the column (, if there are more columns to come, "" otherwise)
    *                          Here is an example how to use it in a template:
    *                            SELECT
    *                            {{#columns}}
    *                            TO_CHAR({{name}}){{trailing_col_char}}
    *                            {{/columns}}
    *                            FROM
    *                            {{table_name}};
    * export_file -> the export file name
    * delta_column -> a delta date column (passed as a Main arg or as a config element), the column which is used to determine new rows for each exports in APPEND mode
    * full_export -> if the export is a full or delta export (the logic is to be implemented in your script)
    *
    * Usage: comet [script-gen] [options]
    *
    * Command: script-gen
    *   --domain <value>            The domain for which to generate extract scripts
    *   --templateFile <value>      Script template file
    *   --scriptsOutputDir <value>  Scripts output folder
    *   --deltaColumn <value>       The date column which is used to determine new rows for each exports (can be passed table by table as config element)
    */
  def run(args: Array[String]): Boolean = {

    val arglist = args.toList
    logger.info(s"Running Comet $arglist")

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
        logger.error(s"One of domain or jobs should be provided")
        false
      case (Nil, jobNames) =>
        val jobs = schemaHandler.jobs
        jobNames
          .map { jobName =>
            // Extracting the Job
            jobs.get(jobName) match {
              case Some(job) =>
                ScriptGen.generateJob(
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
        true
      case (domainNames, Nil) =>
        val domains: List[Domain] = schemaHandler.domains
        domainNames
          .map { domainName =>
            // Extracting the domain from the Excel referential file
            domains.find(_.name == domainName) match {
              case Some(domain) =>
                ScriptGen.generateDomain(
                  domain,
                  config.scriptTemplateFile,
                  config.scriptOutputDir,
                  config.scriptOutputPattern,
                  config.deltaColumn.orElse(ExtractorSettings.deltaColumns.defaultColumn),
                  ExtractorSettings.deltaColumns.deltaColumns
                )
                true
              case None =>
                logger.error(s"No domain found for domain name ${config.domain}")
                false
            }
          }
          .forall(_ == true)
      case (_, _) =>
        logger.error(s"Only one of domain or job list should be passed as an argument")
        false
    }
  }
}

object Main {

  def main(args: Array[String]): Unit = {
    val result = ScriptGen.run(args)
    System.exit(if (result) 0 else 1)
  }
}
