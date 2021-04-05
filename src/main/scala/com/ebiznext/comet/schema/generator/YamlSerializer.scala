package com.ebiznext.comet.schema.generator

import better.files.File
import com.ebiznext.comet.schema.model.Domain
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

object YamlSerializer extends LazyLogging {
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(Include.NON_ABSENT)

  def serialize(domain: Domain): String = mapper.writeValueAsString(domain)

  def serialize(jdbcSchema: JDBCSchema): String = mapper.writeValueAsString(jdbcSchema)

  def deserializeJDBCSchema(file: File) = {
    val rootNode = mapper.readTree(file.newInputStream)
    val extractNode = rootNode.path("jdbc-schema")
    val jdbcNode =
      if (extractNode.isNull() || extractNode.isMissingNode) {
        logger.warn(
          s"Defining a jdbc schema outside a extract node is now deprecated. Please update definition ${file.pathAsString}"
        )
        rootNode
      } else
        extractNode
    mapper.treeToValue(jdbcNode, classOf[JDBCSchema])
  }

  def deserializeDomain(file: File): Domain = {
    scala.io.Source.fromFile(file.pathAsString)
    mapper.readValue(file.newInputStream, classOf[Domain])
  }

  def serializeToFile(targetFile: File, domain: Domain): Unit =
    mapper.writeValue(targetFile.toJava, domain)

  def deserializeDomain(content: String): Try[Domain] = {
    Try {
      val rootNode = mapper.readTree(content)
      val loadNode = rootNode.path("load")
      val domainNode =
        if (loadNode.isNull() || loadNode.isMissingNode) {
          rootNode
        } else
          loadNode
      val domain = mapper.treeToValue(domainNode, classOf[Domain])
      if (domainNode == rootNode)
        logger.warn(
          s"Defining a domain outside a load node is now deprecated. Please update definition fo domain ${domain.name}"
        )

      domain
    }
  }
}
