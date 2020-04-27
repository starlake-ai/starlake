package com.ebiznext.comet.schema.generator

import java.io.File

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.model._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

object SchemaGen extends LazyLogging {
  import YamlSerializer._

  /**
    * Encryption of a data source is done by running a specific ingestion job that aims only to apply Privacy rules on the
    * concerned attributes.
    * To apply the Encryption process on the data sources of a given Domain, we need a corresponding "PreEncryption Domain".
    * The PreEncryption domain contains the same Schemas as the initial Domain but with less constraints on the attributes,
    * which speeds up the encryption process by limiting it to applying the Encryption methods on columns with
    * privacy attributes.
    *
    * @param domain
    */
  def genPreEncryptionDomain(domain: Domain, privacy: Seq[String]): Domain = {
    val preEncryptSchemas: List[Schema] = domain.schemas.map { s =>
      val newAtt =
        s.attributes.map { attr =>
          if (privacy == Nil || privacy.contains(
                attr.privacy.getOrElse(PrivacyLevel.None).toString
              ))
            attr.copy(`type` = "string", required = false, rename = None)
          else
            attr.copy(`type` = "string", required = false, rename = None, privacy = None)
        }
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
        case _ => schema
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

/**
  * Générat the YAML files from the excel sheets s
  */
object Main extends App {
  import SchemaGen._
  implicit val settings: Settings = Settings(ConfigFactory.load())
  val outputPath = settings.comet.metadata
  SchemaGenConfig.parse(args) match {
    case Some(config) =>
      if (config.encryption) {
        for {
          file   <- config.files
          domain <- new XlsReader(file).getDomain()
        } yield {
          val preEncrypt = genPreEncryptionDomain(domain, config.privacy)
          writeDomainYaml(preEncrypt, outputPath, "pre-encrypt-" + preEncrypt.name)
          val postEncrypt = genPostEncryptionDomain(domain)
          writeDomainYaml(postEncrypt, outputPath, "post-encrypt-" + domain.name)
        }
      } else {
        config.files.foreach(generateSchema)
      }
    case _ =>
      println(SchemaGenConfig.usage())
  }
}
