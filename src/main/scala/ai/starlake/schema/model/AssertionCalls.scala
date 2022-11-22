package ai.starlake.schema.model

case class AssertionCalls(assertions: Map[String, String] = Map.empty) {
  def this() = this(Map.empty) // Should never be called. Here for Jackson deserialization only
  val assertionCalls: Map[String, AssertionCall] = {
    assertions.map { case (k, v) =>
      val assertionCall = AssertionCall.fromCall(k, v)
      (assertionCall.name, assertionCall)
    }
  }
}

case class AssertionCall(comment: String, name: String, paramValues: List[String], sql: String)

object AssertionCall {

  def extractNameAndParams(call: String): (String, List[String]) = {
    call
      .split('(') match {
      case Array(n, p) if p.nonEmpty =>
        (n.trim, p.dropRight(1).split(',').map(_.trim).filter(_.nonEmpty).toList)
      case Array(n) =>
        (n, Nil)
      case _ => throw new Exception(s"Invalid assertion call syntax $call")
    }
  }

  def fromCall(comment: String, call: String): AssertionCall = {
    val (name, params) = extractNameAndParams(call)
    AssertionCall(comment, name, params, call)
  }

}
