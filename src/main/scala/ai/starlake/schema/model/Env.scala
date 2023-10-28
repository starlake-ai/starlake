package ai.starlake.schema.model

import ai.starlake.schema.model.Ref.anyRefPattern
import com.fasterxml.jackson.annotation.JsonCreator

import java.util.regex.Pattern

object Ref {
  val anyRefPattern: Pattern = Pattern.compile(".*")
}

case class InputRef(
  table: Pattern = anyRefPattern,
  domain: Option[Pattern] = None,
  database: Option[Pattern] = None
) {
  @JsonCreator
  def this() =
    this(anyRefPattern, None, None) // Should never be called. Here for Jackson deserialization only
}

case class OutputRef(database: String = "", domain: String = "", table: String = "") {
  @JsonCreator
  def this() = this("", "", "") // Should never be called. Here for Jackson deserialization only

  def asTuple(): (String, String, String) = (database, domain, table)

  val tableNamingQuotes = Map(
    Engine.JDBC.toString  -> ("", "."),
    Engine.SPARK.toString -> ("`", ":"),
    Engine.BQ.toString    -> ("`", ".")
  )

  def toSQLString(engine: Engine, isFilesystem: Boolean) = {
    val (quote, separator) =
      tableNamingQuotes.getOrElse(engine.toString, tableNamingQuotes(Engine.JDBC.toString))
    if (!isFilesystem) {
      if (database.isEmpty) {
        if (domain.isEmpty) {
          table
        } else {
          s"$quote$domain.$table$quote"
        }
      } else {
        s"$quote$database$separator$domain.$table$quote"
      }
    } else {
      table
    }
  }
}

case class Ref(
  input: InputRef,
  output: OutputRef
) {
  @JsonCreator
  def this() =
    this(InputRef(), OutputRef()) // Should never be called. Here for Jackson deserialization only
}

case class Refs(refs: List[Ref]) {
  @JsonCreator
  def this() = this(Nil) // Should never be called. Here for Jackson deserialization only

  private def replace(
    ref: OutputRef,
    thisDatabase: String,
    thisDomain: String,
    thisTable: String
  ): OutputRef = {
    ref.copy(
      database = ref.database.replaceAll("SL_THIS_DATABASE", thisDatabase),
      domain = ref.domain.replaceAll("SL_THIS_DOMAIN", thisDomain),
      table = ref.table.replaceAll("SL_THIS_TABLE", thisTable)
    )
  }
  def getOutputRef(database: String, domain: String, table: String): Option[OutputRef] = {
    val result = refs
      .find { ref =>
        (ref.input.database, ref.input.domain) match {
          case (Some(inputDatabase), Some(inputDomain)) =>
            inputDatabase.matcher(database).matches() &&
            inputDomain.matcher(domain).matches() &&
            ref.input.table.matcher(table).matches()
          case _ =>
            false
        }
      }
      .map(_.output)
    result.map(replace(_, database, domain, table))
  }

  def getOutputRef(domain: String, table: String): Option[OutputRef] = {
    val result = refs
      .find { ref =>
        (ref.input.database, ref.input.domain) match {
          case (None, Some(inputDomain)) =>
            inputDomain.matcher(domain).matches() &&
            ref.input.table.matcher(table).matches()
          case _ =>
            false
        }
      }
      .map(_.output)
    result.map(replace(_, "", domain, table))
  }

  def getOutputRef(table: String): Option[OutputRef] = {
    val result = refs
      .find { ref =>
        (ref.input.database, ref.input.domain) match {
          case (None, None) =>
            ref.input.table.matcher(table).matches()
          case _ =>
            false
        }
      }
      .map(_.output)
    result.map(replace(_, "", "", table))

  }

  def getOutputRef(
    components: List[String]
  ): Option[OutputRef] = {
    components match {
      case table :: Nil =>
        getOutputRef(table)
      case domain :: table :: Nil =>
        getOutputRef(domain, table)
      case database :: domain :: table :: Nil =>
        getOutputRef(database, domain, table)
      case _ => None
    }
  }
}

case class Env(
  env: Map[String, String]
) {
  @JsonCreator
  def this() = this(Map.empty) // Should never be called. Here for Jackson deserialization only

}
