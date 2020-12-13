package com.ebiznext.comet.schema.model

case class AssertionCalls(assertions: Map[String, String]) {

  val assertionCalls: Map[String, AssertionCall] = {
    assertions.map { case (k, v) =>
      val assertionCall = AssertionCall.fromCall(k, v)
      (assertionCall.name, assertionCall)
    }
  }
}

case class AssertionCall(comment: String, name: String, params: List[String], sql: String)

object AssertionCall {

  def extractNameAndParams(call: String): (String, List[String]) = {
    call
      .split('(') match {
      case Array(n, p) if p.length >= 1 =>
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
