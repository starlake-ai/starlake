package ai.starlake.schema.model

import com.typesafe.scalalogging.StrictLogging

case class ExpectationDefinitions(expectations: Map[String, String] = Map.empty) {
  def this() = this(Map.empty) // Should never be called. Here for Jackson deserialization only
  val expectationDefinitions: Map[String, ExpectationDefinition] = {
    expectations.map { case (k, v) =>
      val expectationDefinition = ExpectationDefinition.fromDefinition(k, v)
      (expectationDefinition.name, expectationDefinition)
    }
  }
}

case class ExpectationDefinition(
  fullName: String,
  name: String,
  params: List[String],
  sql: String
) {
  def this() =
    this("", "", Nil, "") // Should never be called. Here for Jackson deserialization only
}

object ExpectationDefinition extends StrictLogging {

  def extractNameAndParams(fullName: String): (String, List[String]) = {
    fullName
      .split('(') match {
      case Array(n, p) if p.nonEmpty =>
        (n.trim, p.dropRight(1).split(',').map(_.trim).filter(_.nonEmpty).toList)
      case Array(n) =>
        (n, Nil)
      case _ => throw new Exception(s"Invalid Expectation Definition syntax $fullName")
    }
  }

  def fromDefinition(fullName: String, sql: String): ExpectationDefinition = {
    val (name, params) = extractNameAndParams(fullName)
    logger.info(
      s"Found expectation definition $fullName -> $name(${params.mkString(",")} with SQl $sql"
    )
    ExpectationDefinition(fullName, name, params, sql)
  }
}
