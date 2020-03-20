package com.ebiznext.comet.database.extractor

import better.files.File
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.generator.{SchemaGen, XlsReader}
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
  def templatize(template: File, templateParams: TemplateParams): String = {
    engine.layout(
      template.pathAsString,
      templateParams.paramMap
    )
  }

  /**
    * Generate all extraction scripts based on the given domain
    * @param domain The domain extracted from the Excel referential file
    * @param scriptTemplateFile The script template
    * @param scriptsOutputPath Where the scripts are produced
    * @return The list of produced files
    */
  def generate(domain: Domain, scriptTemplateFile: File, scriptsOutputPath: File): List[File] = {
    val preEncryptionDomain = SchemaGen.genPreEncryptionDomain(domain)
    val templateSettings = TemplateParams.fromDomain(preEncryptionDomain, scriptsOutputPath)
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
  * Generate an extraction scripts based on a given Excel referential file
  * The Excel referential should, at least, specify :
  * - "schema" sheet
  *   - a table name (col A)
  *   - a file pattern (col B) which is used as the export file base name
  *   - a write mode (col D): APPEND or OVERWRITE
  *   - a delta column (col H) if in APPEND mode : the column which is used to determine new rows for each exports
  * - in corresponding source (table) sheets:
  *   - the columns to extract
  *
  * You also have to provide a Mustache (http://mustache.github.io/mustache.5.html) template file.
  *
  * Here you'll write your extraction export process (sqlplus for Oracle, pgsql for PostgreSQL as an example).
  * In that template you can use the following parameters:
  *
  * table_name  -> the table to export
  * delimiter   -> the resulting dsv file delimiter
  * columns     -> the columns to export
  * export_file -> the export file name
  * full_export -> if the export is a full or delta export (the logic is to be implemented in your script)
  *
  * Usage: comet [script-gen] [options]
  *
  * Command: script-gen
  *
  * --referentialFile <value>
  *     Excel referential file
  * --templateFile <value>
  *     Script template file
  * --scriptsOutputDir <value>
  *     Scripts output folder
  */
object Main extends App with StrictLogging {

  import ScriptGen._

  val arglist = args.toList

  logger.info(s"Running Comet $arglist")

  ExtractScriptGenConfig.parse(args) match {
    case Some(config) =>
      // Extracting the domain from the Excel referential file
      val domain: Option[Domain] = new XlsReader(config.referentialFile.pathAsString).getDomain
      domain match {
        case Some(domain) =>
          ScriptGen.generate(domain, config.scriptTemplateFile, config.scriptOutputDir)
          System.exit(0)
        case None =>
          logger.error("Excel referential file was malformed")
          System.exit(1)
      }
    case _ =>
      logger.error("Program execution or parameters are wrong, please check usage")
      System.exit(1)
  }
}
