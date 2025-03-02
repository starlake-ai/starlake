package ai.starlake.schema.model

import com.fasterxml.jackson.annotation.JsonIgnore

case class ExpectationItem(
  expect: String = "",
  failOnError: Boolean = true
) {
  // Should never be called. Here for Jackson deserialization only
  def this() = this("", true)

  def asMap(): Map[String, Any] = Map(
    "expectQuery"       -> expect,
    "expectFailOnError" -> failOnError
  )

  override def toString: String = s"$expect"

  @JsonIgnore
  def name: String = {
    val last = expect.indexOf("(")
    if (last == -1) {
      expect
    } else {
      expect.substring(0, last)
    }
  }

  def params: String = {
    val last = expect.indexOf("(")
    if (last == -1) {
      ""
    } else {
      expect.substring(last + 1, expect.trim.length - 1)
    }
  }

  @JsonIgnore
  def queryCall(): String = "{{" + expect + "}}"
}

case class ExpectationSQL(
  name: String,
  params: String,
  query: String,
  failOnError: Boolean
) {
  def asMap(): Map[String, Any] = {
    Map(
      "name"        -> name,
      "params"      -> params,
      "query"       -> query,
      "failOnError" -> (if (failOnError) "yes" else "no")
    )
  }

}

object ExpectationSQL {
  def apply(expectationItem: ExpectationItem, sql: String): ExpectationSQL = {
    ExpectationSQL(
      expectationItem.name,
      expectationItem.params,
      sql,
      expectationItem.failOnError
    )
  }
}
