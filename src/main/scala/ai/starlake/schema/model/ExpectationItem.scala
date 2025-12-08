package ai.starlake.schema.model

import com.fasterxml.jackson.annotation.JsonIgnore

case class ExpectationItem(
  expect: String = "",
  failOnError: Boolean = true
) {
  // Should never be called. Here for Jackson deserialization only
  def this() = this("", true)

  def asMap(): Map[String, Any] = Map(
    "query"       -> expect,
    "failOnError" -> failOnError.toString
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

  def paramsAsList: List[String] = {
    params.split(",").map(_.trim).toList
  }

  /** Generate a query call for this expectation e.g. {{expectationName(param1,param2)}} If the
    * expectation is namespaced (e.g. category.expectationName(param1,param2)) only the
    * expectationName is kept
    * @return
    *   the query call
    */
  @JsonIgnore
  def queryCall(): String = {
    val nameIndex = expect.indexOf('(')
    val callName =
      if (nameIndex == -1)
        expect
      else {
        val name = expect.substring(0, nameIndex)
        val categoryIndex = name.indexOf('.')
        if (categoryIndex == -1) expect
        else
          expect.substring(categoryIndex + 1)
      }
    "{{" + callName + "}}"
  }
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
