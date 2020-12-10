package com.ebiznext.comet.schema.model

case class AssertionDefinitions(assertions: Map[String, String]) {

  val assertionDefinitions: Map[String, AssertionDefinition] = {
    assertions.map { case (k, v) =>
      val assertionDefinition = AssertionDefinition.fromDefinition(k, v)
      (assertionDefinition.name, assertionDefinition)
    }
  }
}

case class AssertionDefinition(fullName: String, name: String, params: List[String], sql: String) {

  def subst(values: List[String], table: String): String = {
    assert(values.length == params.length)
    (params :+ "comet_table").zip(values :+ table).foldLeft(sql) { case (acc, (p, v)) =>
      s"\\b($p)\\b".r.replaceAllIn(acc, v)
    }
  }
}

object AssertionDefinition {

  def extractNameAndParams(fullName: String): (String, List[String]) = fullName.split('(') match {
    case Array(n, p) if p.length >= 2 =>
      (n, p.trim.drop(1).dropRight(1).split(',').map(_.trim).toList)
    case _ => throw new Exception(s"Invalid Condition syntax $fullName")
  }

  def fromDefinition(fullName: String, sql: String): AssertionDefinition = {
    val (name, params) = extractNameAndParams(fullName)
    AssertionDefinition(fullName, name, params, sql)
  }

}
