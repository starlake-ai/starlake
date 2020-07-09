package com.ebiznext.comet.database.extractor

import better.files.File
import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.schema.handlers.SchemaHandler
import com.ebiznext.comet.schema.model.Domain
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.fusesource.scalate._

object ScriptGen extends StrictLogging {

  implicit val settings: Settings = Settings(ConfigFactory.load())
  val engine: TemplateEngine = new TemplateEngine

  /**
    * Generate an extraction script payload based on a template and its params
    * @param template The extraction script template
    * @param templateParams Its params
    * @return The produced script payload
    */
  def templatize(template: File, templateParams: TemplateParams): String =
    engine.layout(
      template.pathAsString,
      templateParams.paramMap
    )

  /**
    * Generate all extraction scripts based on the given domain
    * @param domain The domain extracted from the Excel referential file
    * @param scriptTemplateFile The script template
    * @param scriptsOutputPath Where the scripts are produced
    * @return The list of produced files
    */
  def generate(
    domain: Domain,
    scriptTemplateFile: File,
    scriptsOutputPath: File,
    deltaColumn: Option[String]
  ): List[File] = {
    val templateSettings = TemplateParams.fromDomain(domain, scriptsOutputPath, deltaColumn)
    templateSettings.map { ts =>
      val scriptPayload = templatize(scriptTemplateFile, ts)
      val scriptFile =
        ts.scriptOutputFile.createFileIfNotExists().overwrite(scriptPayload)
      logger.info(s"Successfully generated script $scriptFile")
      scriptFile
    }
  }

  def printUsage(): Unit = println(ExtractScriptGenConfig.usage)

}

/**
  * Generate extraction scripts based on a given domain
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
  * delta_column -> a delta date column (passed as a Main arg), the column which is used to determine new rows for each exports in APPEND mode
  * full_export -> if the export is a full or delta export (the logic is to be implemented in your script)
  *
  * Usage: comet [script-gen] [options]
  *
  * Command: script-gen
  *   --domain <value>            The domain for which to generate extract scripts
  *   --templateFile <value>      Script template file
  *   --scriptsOutputDir <value>  Scripts output folder
  *   --deltaColumn <value>       The date column which is used to determine new rows for each exports
  */
object Main extends App with StrictLogging {

  import ScriptGen._
  import settings.metadataStorageHandler
  DatasetArea.initMetadata(metadataStorageHandler)
  val schemaHandler = new SchemaHandler(metadataStorageHandler)
  val domains: List[Domain] = schemaHandler.domains

  val arglist = args.toList
  logger.info(s"Running Comet $arglist")

  ExtractScriptGenConfig.parse(args) match {
    case Some(config) =>
      // Extracting the domain from the Excel referential file
      domains.find(_.name == config.domain) match {
        case Some(domain) =>
          ScriptGen.generate(
            domain,
            config.scriptTemplateFile,
            config.scriptOutputDir,
            config.deltaColumn
          )
          System.exit(0)
        case None =>
          logger.error(s"No domain found for domain name ${config.domain}")
          System.exit(1)
      }
    case _ =>
      logger.error("Program execution or parameters are wrong, please check usage")
      System.exit(1)
  }
}
