package com.ebiznext.comet.schema.generator

import java.util.regex.Pattern

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.schema.model._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

object SchemaGen extends LazyLogging {

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
          if (
            privacy == Nil || privacy.contains(
              attr.privacy.getOrElse(PrivacyLevel.None).toString
            )
          )
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
    *     - Separator : µ
    * @param domain
    */
  def genPostEncryptionDomain(
    domain: Domain,
    delimiter: Option[String],
    privacy: Seq[String]
  ): Domain = {
    val postEncryptSchemas: List[Schema] = domain.schemas.map { schema =>
      val metadata = for {
        metadata <- schema.metadata
        format   <- metadata.format
      } yield {
        if (!List(Format.SIMPLE_JSON, Format.DSV, Format.POSITION).contains(format))
          throw new Exception("Not Implemented")
        metadata.copy(
          format = Some(Format.DSV),
          separator = delimiter.orElse(schema.metadata.flatMap(_.separator)).orElse(Some("µ")),
          withHeader = schema.metadata.flatMap(_.withHeader)
        )
      }
      val attributes = schema.attributes.map { attr =>
        if (
          privacy == Nil || privacy.contains(
            attr.privacy.getOrElse(PrivacyLevel.None).toString
          )
        )
          attr.copy(privacy = None)
        else
          attr
      }
      schema.copy(
        metadata = metadata,
        attributes = attributes,
        pattern = Pattern.compile(s"${schema.name}.csv")
      )
    }
    val postEncryptDomain = domain.copy(schemas = postEncryptSchemas)
    postEncryptDomain
  }

  def generateSchema(inputPath: String, outputPath: Option[String] = None)(implicit
    settings: Settings
  ): Unit = {
    val reader = new XlsReader(inputPath)
    reader.getDomain.foreach { domain =>
      writeDomainYaml(domain, outputPath.getOrElse(DatasetArea.domains.toString), domain.name)
    }
  }

  def writeDomainYaml(domain: Domain, outputPath: String, fileName: String): Unit = {
    import java.io.File

    import YamlSerializer._
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
  val defaultOutputPath = DatasetArea.domains.toString
  SchemaGenConfig.parse(args) match {
    case Some(config) =>
      if (config.encryption) {
        for {
          file   <- config.files
          domain <- new XlsReader(file).getDomain()
        } yield {
          val preEncrypt = genPreEncryptionDomain(domain, config.privacy)
          writeDomainYaml(
            preEncrypt,
            config.outputPath.getOrElse(defaultOutputPath),
            "pre-encrypt-" + preEncrypt.name
          )
          val postEncrypt = genPostEncryptionDomain(domain, config.delimiter, config.privacy)
          writeDomainYaml(
            postEncrypt,
            config.outputPath.getOrElse(defaultOutputPath),
            "post-encrypt-" + domain.name
          )
        }
      } else {
        config.files.foreach(generateSchema(_, config.outputPath))
      }
    case _ =>
      println(SchemaGenConfig.usage())
  }
}
