package com.ebiznext.comet.schema.generator

import java.io.File

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

  def serializeToFile(targetFile: File, domain: Domain): Unit =
    mapper.writeValue(targetFile, domain)
}
