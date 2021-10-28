package com.ebiznext.comet.schema.generator

import better.files.File
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.model.Domain
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success}

object DDL2Yml extends LazyLogging {

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    DDL2YmlConfig.parse(args) match {
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
  def run(config: DDL2YmlConfig)(implicit settings: Settings): Unit = {
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
    val selectedTablesAndColumns = DDLUtils.extractJDBCTables(jdbcSchema)
    DDLUtils.extractDomain(jdbcSchema, domainTemplate, selectedTablesAndColumns)
  }

  def main(args: Array[String]): Unit = {
    DDL2Yml.run(args)
  }
}
