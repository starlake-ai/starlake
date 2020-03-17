package com.ebiznext.comet.schema.generator

import java.io.File

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.model._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

object SchemaGen extends LazyLogging {
  import YamlSerializer._

  def printUsage(): Unit = {
    println("""
        |Usage:
        |- To generate Yaml for a domain with no encryption:
        |SchemaGen generate-yml <Excel file>
        |- To generate Yaml for a domain with encryption:
        |SchemaGen generate-encryptionYml <Excel file>
        |""".stripMargin)
  }

  /**
    * build pre encryption Domain => for each attribute of each schema set :
    *     - type to "string"
    *     - required to False
    *     - rename to None
    * @param domain
    */
  def genPreEncryptionDomain(domain: Domain): Domain = {
    val preEncryptSchemas: List[Schema] = domain.schemas.map { s =>
      val newAtt = s.attributes.map(_.copy(`type` = "string", required = false, rename = None))
      s.copy(attributes = newAtt)
    }
    val preEncryptDomain = domain.copy(schemas = preEncryptSchemas)
    preEncryptDomain
  }

  /**
    * build post encryption Domain => for each Position schema update its Metadata as follows
    *     - Format : DSV
    *     - With Header : False
    *     - Separator : µ  //TODO perhaps read this from reference.conf
    * @param domain
    */
  def genPostEncryptionDomain(domain: Domain): Domain = {
    val postEncryptSchemas: List[Schema] = domain.schemas.map { schema =>
      schema.metadata.flatMap(_.format) match {
        case Some(Format.POSITION) => {
          val postEncryptMetaData = schema.metadata.map(
            _.copy(
              format = Some(Format.DSV),
              withHeader = Some(false), //TODO set to true, and make sure files are written with a header ?
              separator = Some("µ")
            )
          )
          schema.copy(metadata = postEncryptMetaData)
        }
        case Some(_) => schema
        case None    => schema
      }
    }
    val postEncryptDomain = domain.copy(schemas = postEncryptSchemas)
    postEncryptDomain
  }

  def generateSchema(path: String)(implicit settings: Settings): Unit = {
    val reader = new XlsReader(path)
    reader.getDomain.foreach { domain =>
      writeDomainYaml(domain, settings.comet.metadata, domain.name)
    }
  }

  def writeDomainYaml(domain: Domain, outputPath: String, fileName: String): Unit = {
    logger.info(s"""Generated schemas:
                   |${serialize(domain)}""".stripMargin)
    serializeToFile(new File(outputPath, s"${fileName}.yml"), domain)
  }

}

object Main extends App {
  import SchemaGen._
  implicit val settings: Settings = Settings(ConfigFactory.load())

  if (args.length == 0) printUsage()
  else {
    val arglist = args.toList
    val outputPath = settings.comet.metadata
    arglist.head match {
      case "generate-yml" if arglist.length == 2 => generateSchema(arglist(1))
      case "generate-encryptionYml" if arglist.length == 2 => {
        val domainOpt = new XlsReader(arglist(1)).getDomain()
        domainOpt.foreach { d =>
          val preEncrypt = genPreEncryptionDomain(d)
          writeDomainYaml(preEncrypt, outputPath, "preEncrypt-" + preEncrypt.name)
          val postEncrypt = genPostEncryptionDomain(d)
          writeDomainYaml(postEncrypt, outputPath, "postEncrypt-" + d.name)
        }
      }
      case _ => printUsage()
    }
  }
}
