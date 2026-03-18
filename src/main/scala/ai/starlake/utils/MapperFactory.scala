package ai.starlake.utils

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonSetter, Nulls}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object MapperFactory {

  def newYamlMapper(): ObjectMapper = {
    val mapper = new ObjectMapper(new YAMLFactory())
    setMapperProperties(mapper)
  }

  def newJsonMapper(): ObjectMapper = {
    val mapper = new ObjectMapper()
    setMapperProperties(mapper)
  }

  def setMapperProperties(mapper: ObjectMapper): ObjectMapper = {
    mapper
      .registerModule(DefaultScalaModule)
      .registerModule(new HadoopModule())
      .registerModule(new StorageLevelModule())
      .setSerializationInclusion(Include.NON_EMPTY)
      .setDefaultSetterInfo(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY, Nulls.AS_EMPTY))
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper
  }
}
