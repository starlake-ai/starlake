package ai.starlake.utils

import ai.starlake.schema.model.{ListDiff, Named}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging

object JsonSerializer extends LazyLogging {
  val mapper: ObjectMapper = new ObjectMapper()
  Utils.setMapperProperties(mapper)

  def serializeDiffStrings(diffs: ListDiff[String]): Option[String] =
    if (diffs.isEmpty()) None else Some(mapper.writeValueAsString(diffs))

  def serializeDiffNamed(diffs: ListDiff[Named]): Option[String] =
    if (diffs.isEmpty()) None else Some(mapper.writeValueAsString(diffs))

  def serializeObject(obj: Object): String = mapper.writeValueAsString(obj)
}
