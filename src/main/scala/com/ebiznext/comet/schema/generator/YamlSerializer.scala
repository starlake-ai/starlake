package com.ebiznext.comet.schema.generator

import better.files.File
import com.ebiznext.comet.schema.model.Domain
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object YamlSerializer {
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(Include.NON_ABSENT)

  def serialize(domain: Domain): String = mapper.writeValueAsString(domain)
  def serialize(jdbcSchema: JDBCSchema): String = mapper.writeValueAsString(jdbcSchema)

  def deserializeJDBCSchema(file: File): JDBCSchema =
    mapper.readValue(file.newInputStream, classOf[JDBCSchema])

  def deserializeDomain(file: File): Domain =
    mapper.readValue(file.newInputStream, classOf[Domain])

  def serializeToFile(targetFile: File, domain: Domain): Unit =
    mapper.writeValue(targetFile.toJava, domain)
}
