package ai.starlake.schema.model

import ai.starlake.schema.model.Ref.anyRefPattern
import com.fasterxml.jackson.annotation.JsonCreator

import java.util.regex.Pattern

object Ref {
  val anyRefPattern: Pattern = Pattern.compile(".*")
}

case class Ref(table: Pattern = anyRefPattern, domain: Pattern = anyRefPattern) {
  @JsonCreator
  def this() = this(
    anyRefPattern,
    anyRefPattern
  ) // Should never be called. Here for Jackson deserialization only
}

case class RefsRule(
  ref: List[Ref] = Nil,
  domain: Option[String] = None,
  database: Option[String] = None
) {
  @JsonCreator
  def this() =
    this(Nil, None, None) // Should never be called. Here for Jackson deserialization only
}

case class Env(
  env: Map[String, String],
  refs: List[RefsRule]
) {
  @JsonCreator
  def this() = this(Map.empty, Nil) // Should never be called. Here for Jackson deserialization only

  def getDatabase(domain: String, table: String): Option[String] = {
    val matchedRefsRule: Option[RefsRule] = refs.flatMap { refsRule =>
      val refRule: Option[Ref] = refsRule.ref.find { refRule =>
        refRule.domain.matcher(domain).matches() && refRule.table.matcher(table).matches()
      }
      refRule match {
        case Some(_) => Some(refsRule)
        case _       => None
      }
    }.headOption

    matchedRefsRule match {
      case Some(matchedRefsRule) => matchedRefsRule.database
      case _                     => None
    }
  }

  def getDomainAndDatabase(table: String): (Option[String], Option[String]) = {

    val matchedRefsRule: Option[RefsRule] = refs.flatMap { refsRule =>
      val refRule: Option[Ref] = refsRule.ref.find { refRule =>
        refRule.domain.toString == anyRefPattern.toString &&
        refsRule.domain.isDefined && refRule.table.matcher(table).matches()
      }
      refRule match {
        case Some(_) => Some(refsRule)
        case _       => None
      }
    }.headOption

    val (domain, database) = matchedRefsRule match {
      case Some(matchedRefsRule) => (matchedRefsRule.domain, matchedRefsRule.database)
      case _                     => (None, None)
    }
    (domain, database) match {
      case (Some(_), Some(_)) => (domain, database)
      case (Some(dom), None)  => (domain, getDatabase(dom, table))
      case (None, _)          => (None, None)
    }
  }

}
