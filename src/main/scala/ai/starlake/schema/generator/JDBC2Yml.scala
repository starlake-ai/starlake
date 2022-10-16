package ai.starlake.schema.generator

import better.files.File
import ai.starlake.config.Settings
import ai.starlake.schema.model.{Domain, Schemas}
import ai.starlake.utils.YamlSerializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success}

object JDBC2Yml extends LazyLogging {

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    JDBC2YmlConfig.parse(args) match {
      case Some(config) =>
        run(config)
      case None =>
        throw new Exception(s"Could not parse arguments ${args.mkString(" ")}")
    }
  }

  /** Generate YML file from JDBC Schema stored in a YML file
    *
    * @param jdbcMapFile
    *   : Yaml File containing the JDBC Schema to extract
    * @param ymlOutputDir
    *   : Where to output the YML file. The generated filename will be in the for
    *   TABLE_SCHEMA_NAME.yml
    * @param settings
    *   : Application configuration file
    */
  def run(config: JDBC2YmlConfig)(implicit settings: Settings): Unit = {
    val jdbcSchemas =
      YamlSerializer.deserializeJDBCSchemas(File(config.jdbcMapping))
    config.mode match {
      case "data" =>
        jdbcSchemas.jdbcSchemas.foreach { jdbcSchema =>
          extractData(jdbcSchema, File(config.outputDir), config.limit, config.separator)
        }
      case "schema" =>
        jdbcSchemas.jdbcSchemas.foreach { jdbcSchema =>
          val domainTemplate = config.ymlTemplate.orElse(jdbcSchema.template).map { ymlTemplate =>
            YamlSerializer.deserializeDomain(File(ymlTemplate)) match {
              case Success(domain) =>
                domain.metadata match {
                  case Some(metadata) if metadata.directory.isEmpty =>
                    throw new Exception(
                      "Domain metadata directory property is mandatory in template file."
                    )
                  case Some(_) =>
                    domain
                }
                domain
              case Failure(e) => throw e
            }
          }
          extractSchema(jdbcSchema, File(config.outputDir), domainTemplate)
        }
    }
  }

  def extractData(jdbcSchema: JDBCSchema, outputDir: File, limit: Int, separator: String)(implicit
    settings: Settings
  ): Unit = {
    JDBCUtils.extractData(jdbcSchema, outputDir, limit, separator)
  }

  def extractSchema(jdbcSchema: JDBCSchema, baseOutputDir: File, domainTemplate: Option[Domain])(
    implicit settings: Settings
  ): Unit = {

    val domainName = jdbcSchema.schema.replaceAll("[^\\p{Alnum}]", "_")
    File(baseOutputDir, domainName).createDirectories()
    val domain = extractDomain(jdbcSchema, domainTemplate)
    val tables = domain.tables
    val tableRefs = tables.map("_" + _.name)
    tables.foreach { table =>
      val content = YamlSerializer.serialize(Schemas(List(table)))
      val file = File(baseOutputDir, domainName, "_" + table.name + ".comet.yml")
      file.overwrite(content)
    }
    val finalDomain = domain.copy(tableRefs = Some(tableRefs), tables = Nil)
    YamlSerializer.serializeToFile(
      File(baseOutputDir, domainName, domainName + ".comet.yml"),
      finalDomain
    )
  }

  /** Generate YML file from the JDBCSchema
    *
    * @param jdbcSchema
    *   : the JDBC Schema to extract
    * @param settings
    *   : Application configuration file
    */
  def extractDomain(jdbcSchema: JDBCSchema, domainTemplate: Option[Domain])(implicit
    settings: Settings
  ): Domain = {
    val selectedTablesAndColumns = JDBCUtils.extractJDBCTables(jdbcSchema)
    JDBCUtils.extractDomain(jdbcSchema, domainTemplate, selectedTablesAndColumns)
  }

  def main(args: Array[String]): Unit = {
    JDBC2Yml.run(args)
  }
}
