package ai.starlake.utils

import ai.starlake.schema.model.{ListDiff, Named}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging

object JsonSerializer extends LazyLogging {
  val mapper: ObjectMapper = new ObjectMapper()
  Utils.setMapperProperties(mapper)

  def serializeDiffStrings(diffs: ListDiff[String]): String = mapper.writeValueAsString(diffs)

  def serializeDiffNamed(diffs: ListDiff[Named]): String = mapper.writeValueAsString(diffs)

  def serializeObject(obj: Object): String = mapper.writeValueAsString(obj)
}
