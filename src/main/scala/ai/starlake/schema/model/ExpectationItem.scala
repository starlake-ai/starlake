package ai.starlake.schema.model

import com.fasterxml.jackson.annotation.JsonIgnore

case class ExpectationItem(
  expect: String = "",
  failOnError: Boolean = true
) {
  // Should never be called. Here for Jackson deserialization only
  def this() = this("", true)

  override def toString: String = s"$expect"

  @JsonIgnore
  def name: String = expect.replaceAll("[\"']", "").replaceAll("[^a-zA-Z0-9]", "_");

  @JsonIgnore
  def queryCall(): String = "{{" + expect + "}}"
}
