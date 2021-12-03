package ai.starlake.schema.generator

import better.files.File
import ai.starlake.config.Settings
import ai.starlake.schema.model.Domain
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
    jdbcSchemas.jdbcSchemas.foreach { jdbcSchema =>
      val domainTemplate = config.ymlTemplate.orElse(jdbcSchema.template).map { ymlTemplate =>
        YamlSerializer.deserializeDomain(File(ymlTemplate)) match {
          case Success(domain) => domain
          case Failure(e)      => throw e
        }
      }
      run(jdbcSchema, File(config.outputDir), domainTemplate)
    }
  }

  def run(jdbcSchema: JDBCSchema, outputDir: File, domainTemplate: Option[Domain])(implicit
    settings: Settings
  ): Unit = {
    val domain = run(jdbcSchema, domainTemplate)
    YamlSerializer.serializeToFile(
      File(outputDir, jdbcSchema.schema + ".comet.yml"),
      domain
    )
  }

  /** Generate YML file from the JDBCSchema
    *
    * @param jdbcSchema
    *   : the JDBC Schema to extract
    * @param settings
    *   : Application configuration file
    */
  def run(jdbcSchema: JDBCSchema, domainTemplate: Option[Domain])(implicit
    settings: Settings
  ): Domain = {
    val selectedTablesAndColumns = JDBCUtils.extractJDBCTables(jdbcSchema)
    JDBCUtils.extractDomain(jdbcSchema, domainTemplate, selectedTablesAndColumns)
  }

  def main(args: Array[String]): Unit = {
    JDBC2Yml.run(args)
  }
}
