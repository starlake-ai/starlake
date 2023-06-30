package ai.starlake.schema.model

case class ExpectationCalls(expectations: Map[String, String] = Map.empty) {
  def this() = this(Map.empty) // Should never be called. Here for Jackson deserialization only
  val expectationCalls: Map[String, ExpectationCall] = {
    expectations.map { case (k, v) =>
      val expectationCall = ExpectationCall.fromCall(k, v)
      (expectationCall.name, expectationCall)
    }
  }
}

case class ExpectationCall(comment: String, name: String, paramValues: List[String], sql: String)

object ExpectationCall {

  def extractNameAndParams(call: String): (String, List[String]) = {
    call
      .split('(') match {
      case Array(n, p) if p.nonEmpty =>
        (n.trim, p.dropRight(1).split(',').map(_.trim).filter(_.nonEmpty).toList)
      case Array(n) =>
        (n, Nil)
      case _ => throw new Exception(s"Invalid expectation call syntax $call")
    }
  }

  def fromCall(comment: String, call: String): ExpectationCall = {
    val (name, params) = extractNameAndParams(call)
    ExpectationCall(comment, name, params, call)
  }

}
