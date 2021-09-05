package com.ebiznext.comet.schema.model

import com.typesafe.scalalogging.StrictLogging

case class AssertionDefinitions(assertions: Map[String, String]) {

  val assertionDefinitions: Map[String, AssertionDefinition] = {
    assertions.map { case (k, v) =>
      val assertionDefinition = AssertionDefinition.fromDefinition(k, v)
      (assertionDefinition.name, assertionDefinition)
    }
  }
}

case class AssertionDefinition(fullName: String, name: String, params: List[String], sql: String)

object AssertionDefinition extends StrictLogging {

  def extractNameAndParams(fullName: String): (String, List[String]) = {
    fullName
      .split('(') match {
      case Array(n, p) if p.nonEmpty =>
        (n.trim, p.dropRight(1).split(',').map(_.trim).filter(_.nonEmpty).toList)
      case Array(n) =>
        (n, Nil)
      case _ => throw new Exception(s"Invalid Assertion Definition syntax $fullName")
    }
  }

  def fromDefinition(fullName: String, sql: String): AssertionDefinition = {
    val (name, params) = extractNameAndParams(fullName)
    logger.info(
      s"Found assertion definition $fullName -> $name(${params.mkString(",")} with SQl $sql"
    )
    AssertionDefinition(fullName, name, params, sql)
  }

}
