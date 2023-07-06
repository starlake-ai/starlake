package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.schema.model.Ref.anyRefPattern
import com.fasterxml.jackson.annotation.JsonCreator
import ai.starlake.utils.Formatter._

import java.util.regex.Pattern

object Ref {
  val anyRefPattern: Pattern = Pattern.compile(".*")
}

case class Ref(
  table: Pattern = anyRefPattern,
  domain: Pattern = anyRefPattern,
  rename: Option[String]
) {
  @JsonCreator
  def this() = this(
    anyRefPattern,
    anyRefPattern,
    None
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

  def richFormat(activeEnv: Map[String, String], extraEnv: Map[String, String])(implicit
    settings: Settings
  ): RefsRule = {
    RefsRule(
      ref,
      domain.map(_.richFormat(activeEnv, extraEnv)),
      database.map(_.richFormat(activeEnv, extraEnv))
    )
  }
}

case class EnvRefs(refs: List[RefsRule]) {
  def getDatabase(domain: String, table: String): Option[String] = {
    val matchedRefsRule: Option[RefsRule] = refs.find { refsRule =>
      refsRule.ref.exists { refRule =>
        refRule.domain.matcher(domain).matches() && refRule.table.matcher(table).matches()
      }
    }
    matchedRefsRule.flatMap(_.database)
  }

  def getDomainAndDatabase(table: String): (Option[String], Option[String]) = {

    val matchedRefsRule: Option[RefsRule] = refs.flatMap { refsRule =>
      val refRule: Option[Ref] = refsRule.ref.find { refRule =>
        refsRule.domain match {
          case None =>
            refRule.table.matcher(table).matches()
          case Some(domain) if domain == anyRefPattern.toString =>
            refRule.table.matcher(table).matches()
          case _ =>
            false
        }
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

  def getDatabaseAndDomainAndTable(
    components: List[String]
  ): Option[(Option[String], Option[String], String)] = {
    components match {
      case table :: Nil =>
        val (domain, database) = getDomainAndDatabase(table)
        Some((database, domain, table))
      case domain :: table :: Nil =>
        val database = getDatabase(domain, table)
        Some((database, Some(domain), table))
      case database :: domain :: table :: Nil =>
        Some((Some(database), Some(domain), table))
      case _ => None
    }

  }
}

case class Env(
  env: Map[String, String],
  refs: List[RefsRule]
) {
  @JsonCreator
  def this() = this(Map.empty, Nil) // Should never be called. Here for Jackson deserialization only

}
