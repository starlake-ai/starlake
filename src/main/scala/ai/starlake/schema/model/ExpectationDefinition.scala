package ai.starlake.schema.model

import ai.starlake.utils.CompilerUtils
import com.typesafe.scalalogging.StrictLogging

import scala.util.Try

case class ExpectationItem(query: String, expect: String) {
  def this() = this("", "") // Should never be called. Here for Jackson deserialization only
}
case class ExpectationDefinitions(expectations: Map[String, ExpectationItem] = Map.empty) {
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
  expectation: ExpectationItem
) {
  def this() =
    this(
      "",
      "",
      Nil,
      ExpectationItem("", "true")
    ) // Should never be called. Here for Jackson deserialization only

  def checkValidity(): List[ValidationMessage] = {
    val (name, params) = ExpectationDefinition.extractNameAndParams(fullName)
    val sampleExpectation = params.fold(expectation.expect) { (acc, param) =>
      acc.replace(param, "1")
    }
    Try {
      CompilerUtils.compile(sampleExpectation)
    } match {
      case scala.util.Success(_) => Nil
      case scala.util.Failure(e) =>
        ValidationMessage(
          Severity.Warning,
          fullName,
          s"Could not validate expectation $fullName in $sampleExpectation : ${e.getMessage}"
        ) :: Nil
    }
  }
}

object ExpectationDefinition extends StrictLogging {

  /** Extracts the name and parameters from a full expectation definition
    * @param fullName
    * @return
    *   (name, params)
    */
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

  def fromDefinition(fullName: String, item: ExpectationItem): ExpectationDefinition = {
    val (name, params) = extractNameAndParams(fullName)
    logger.info(
      s"Found expectation definition $fullName -> $name(${params.mkString(",")}) with SQL ${item.query}"
    )
    ExpectationDefinition(fullName, name, params, item)
  }

  def checkValidity(
    expectationDefinitions: List[ExpectationDefinition]
  ): List[ValidationMessage] = {
    expectationDefinitions.flatMap(_.checkValidity())
  }
}
