package ai.starlake.extract

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Domain
import better.files.File
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.fusesource.scalate._

class ExtractScript(schemaHandler: SchemaHandler)(implicit settings: Settings)
    extends StrictLogging {
  val engine: TemplateEngine = new TemplateEngine

  private def formatOutputScriptName(name: String, templateParams: TemplateParams): String = {
    val vars = schemaHandler.cometDateVars ++ Map(
      "domain" -> templateParams.domainToExport,
      "table"  -> templateParams.tableToExport
    )
    vars.foldLeft(name.substring(0, name.lastIndexOf("."))) { case (a, (cometVar, cometVal)) =>
      a.replace(cometVar, cometVal)
    }
  }

  /** Generate an extraction script payload based on a template and its params
    *
    * @param templateName
    *   The extraction script template
    * @param templateParams
    *   Its params
    * @return
    *   The produced script payload
    */
  def templatize(templateName: String, templateParams: TemplateParams): List[File] = {
    val templateFolder = File(new Path(DatasetArea.extract, templateName).toString)
    templatizeFolder(templateFolder, templateParams)
  }

  def templatizeFolder(templateFolder: File, templateParams: TemplateParams): List[File] = {
    val filesPath = if (templateFolder.isDirectory) {
      templateFolder.list
        .map(_.pathAsString)
        .filter(name => name.endsWith(".ssp") || name.endsWith(".mustache"))
    } else {
      throw new Exception(s"Invalid template folder ${templateFolder.pathAsString}")
    }

    filesPath.map { inputPath =>
      templatizeFile(inputPath, templateParams)
    }.toList
  }

  def templatizeFile(inputPath: String, templateParams: TemplateParams): File = {
    val scriptPayload = engine.layout(
      inputPath,
      templateParams.paramMap
    )
    val outputPath = formatOutputScriptName(inputPath, templateParams)
    val outputFile = File(outputPath)
    val generatedOutputFile = File(outputFile.parent, "generated", outputFile.name)
    generatedOutputFile.parent.createDirectoryIfNotExists()
    generatedOutputFile.createFileIfNotExists().overwrite(scriptPayload)

    logger.info(s"Successfully generated script $generatedOutputFile")
    generatedOutputFile
  }

  /** Generate all extraction scripts based on the given domain
    *
    * @param domain
    *   The domain extracted from the Excel referential file
    * @param scriptTemplateName
    *   The script template
    * @param defaultDeltaColumn
    *   Defaut delta column
    * @param deltaColumns
    *   Mapping table name -> delta column, has precedence over `defaultDeltaColumn`
    * @return
    *   The list of produced files
    */
  def generateDomain(
    domain: Domain,
    scriptTemplateName: String,
    defaultDeltaColumn: Option[String],
    deltaColumns: Map[String, String],
    auditDB: Option[String],
    activeEnv: Map[String, String]
  ): List[File] = {
    val templateSettings =
      TemplateParams.fromDomain(
        domain,
        defaultDeltaColumn,
        deltaColumns,
        auditDB,
        activeEnv
      )
    templateSettings.flatMap { ts =>
      templatize(scriptTemplateName, ts)
    }
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
    *     TO_CHAR({{name}}){{trailing_col_char}} {{/columns}} FROM {{table_name}}; delta_column -> a
    *     delta date column (passed as a Main arg or as a config element), the column which is used
    *     to determine new rows for each exports in APPEND mode full_export -> if the export is a
    *     full or delta export (the logic is to be implemented in your script)
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

    ExtractScriptConfig.parse(args) match {
      case Some(config) =>
        run(config)
      case _ =>
        logger.error("Program execution or parameters are wrong, please check usage")
        false
    }
  }

  def run(config: ExtractScriptConfig)(implicit settings: Settings): Boolean = {
    DatasetArea.initMetadata(settings.storageHandler)
    val schemaHandler = new SchemaHandler(settings.storageHandler)
    config.domain match {
      case Nil =>
        logger.warn(s"No domain or jobs provided. Extracting all domains")
        val domainNames = schemaHandler.domains().map(_.name)
        runOnDomains(config, schemaHandler, domainNames)
      case domainNames =>
        runOnDomains(config, schemaHandler, domainNames)
    }
  }

  private def runOnDomains(
    config: ExtractScriptConfig,
    schemaHandler: SchemaHandler,
    domainNames: Seq[String]
  ): Boolean = {
    val domains: List[Domain] = schemaHandler.domains()
    domainNames
      .map { domainName =>
        // Extracting the domain from the Excel referential file
        domains.find(_.name == domainName) match {
          case Some(domain) =>
            generateDomain(
              domain,
              config.scriptTemplateName,
              config.deltaColumn.orElse(ExtractorSettings.deltaColumns.defaultColumn),
              ExtractorSettings.deltaColumns.deltaColumns,
              Some(config.auditDB),
              schemaHandler.activeEnv()
            )
            true
          case None =>
            logger.error(s"No domain found for domain name ${config.domain}")
            false
        }
      }
      .forall(_ == true)
  }
}

object Main {
  implicit val settings: Settings = Settings(ConfigFactory.load())
  import settings.storageHandler
  DatasetArea.initMetadata(storageHandler)
  val schemaHandler = new SchemaHandler(storageHandler)

  def main(args: Array[String]): Unit = {
    val result = new ExtractScript(schemaHandler).run(args)
    if (!result) throw new Exception("ScriptGen failed!")
  }
}
