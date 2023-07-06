package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.utils.Formatter._
import com.fasterxml.jackson.annotation.JsonCreator

import java.util.regex.Pattern

object Ref {
  val anyRefPattern: Pattern = Pattern.compile(".*")
}

case class InputRef(
  table: String = "",
  domain: Option[String] = None,
  database: Option[String] = None
) {
  @JsonCreator
  def this() = this("", None, None) // Should never be called. Here for Jackson deserialization only

  def richFormat(activeEnv: Map[String, String], extraEnv: Map[String, String])(implicit
    settings: Settings
  ): InputRef = {
    InputRef(
      table.richFormat(activeEnv, extraEnv),
      domain.map(_.richFormat(activeEnv, extraEnv)),
      database.map(_.richFormat(activeEnv, extraEnv))
    )
  }
}

case class OutputRef(table: String = "", domain: String = "", database: String = "") {
  @JsonCreator
  def this() = this("", "", "") // Should never be called. Here for Jackson deserialization only

  def richFormat(activeEnv: Map[String, String], extraEnv: Map[String, String])(implicit
    settings: Settings
  ): OutputRef = {
    OutputRef(
      table.richFormat(activeEnv, extraEnv),
      domain.richFormat(activeEnv, extraEnv),
      database.richFormat(activeEnv, extraEnv)
    )
  }
  def asTuple(): (String, String, String) = (database, domain, table)
  def toBigQueryString() = s"`$database.$domain.$table`"
}

case class Ref(
  input: InputRef,
  output: OutputRef
) {
  @JsonCreator
  def this() =
    this(InputRef(), OutputRef()) // Should never be called. Here for Jackson deserialization only

  def richFormat(activeEnv: Map[String, String], extraEnv: Map[String, String])(implicit
    settings: Settings
  ): Ref = {
    Ref(
      input.richFormat(activeEnv, extraEnv),
      output.richFormat(activeEnv, extraEnv)
    )
  }
}

case class EnvRefs(refs: List[Ref])(implicit settngs: Settings) {

  private def richFormat(
    ref: OutputRef,
    thisDatabase: String,
    thisDomain: String,
    thisTable: String
  ): OutputRef = {
    ref.copy(
      database = ref.database.richFormat(Map("SL_THIS_DATABASE" -> thisDatabase), Map.empty),
      domain = ref.domain.richFormat(Map("SL_THIS_DOMAIN" -> thisDomain), Map.empty),
      table = ref.table.richFormat(Map("SL_THIS_TABLE" -> thisTable), Map.empty)
    )
  }
  def getOutputRef(database: String, domain: String, table: String): Option[OutputRef] = {
    val result = refs
      .find { ref =>
        (ref.input.database, ref.input.domain) match {
          case (Some(inputDatabase), Some(inputDomain)) =>
            inputDatabase.equalsIgnoreCase(database) &&
            inputDomain.equalsIgnoreCase(domain) &&
            ref.input.table.equalsIgnoreCase(table)
          case _ =>
            false
        }
      }
      .map(_.output)
    result.map(richFormat(_, database, domain, table))
  }

  def getOutputRef(domain: String, table: String): Option[OutputRef] = {
    val result = refs
      .find { ref =>
        ref.input.domain match {
          case Some(inputDomain) =>
            inputDomain.equalsIgnoreCase(domain) &&
            ref.input.table.equalsIgnoreCase(table)
          case None =>
            false
        }
      }
      .map(_.output)
    result.map(richFormat(_, "", domain, table))
  }

  def getOutputRef(table: String): Option[OutputRef] = {
    val result = refs
      .find { ref =>
        ref.input.domain match {
          case None =>
            ref.input.table.equalsIgnoreCase(table)
          case _ =>
            false
        }
      }
      .map(_.output)
    result.map(richFormat(_, "", "", table))

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
  env: Map[String, String],
  refs: List[Ref]
) {
  @JsonCreator
  def this() = this(Map.empty, Nil) // Should never be called. Here for Jackson deserialization only

}
