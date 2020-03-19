package com.ebiznext.comet.oracle.generator

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
    * Generate an extraction script based on a template and its params
    * @param template The extraction script template
    * @param templateParams Its params
    * @return The produced script file
    */
  def templatize(template: File, templateParams: TemplateParams): File = {
    val scriptPayload = engine.layout(
      template.pathAsString,
      templateParams.paramMap
    )

    val scriptFile =
      templateParams.scriptOutputFile.createFileIfNotExists().overwrite(scriptPayload)
    logger.info(s"Successfully generated script $scriptFile")
    scriptFile
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
    templateSettings.map(templatize(scriptTemplateFile, _))
  }

  def printUsage(): Unit = println(ScriptGenConfig.usage)

}

/**
  * Generate Oracle sqlplus extraction scripts based on a given Excel referential file
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

  ScriptGenConfig.parse(args) match {
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
