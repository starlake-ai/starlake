package ai.starlake.schema.model

import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.Ref.anyRefPattern
import ai.starlake.utils.{Utils, YamlSerde}
import com.fasterxml.jackson.annotation.JsonCreator
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import java.time.LocalDateTime
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

  private val tableNamingQuotes = Map(
    Engine.JDBC.toString  -> ("", "."),
    Engine.SPARK.toString -> ("`", ":"),
    Engine.BQ.toString    -> ("`", ".")
  )

  def toSQLString(connection: Settings.Connection): String = {
    val engine =
      if (connection.`type` == ConnectionType.BQ)
        Engine.BQ
      else if (connection.`type` == ConnectionType.JDBC)
        Engine.JDBC
      else
        Engine.SPARK

    val (quote, separator) =
      (connection.quote, connection.separator) match {
        case (Some(quote), Some(separator)) =>
          (quote, separator)
        case (Some(quote), None) =>
          val (ignoreQuote, separator) =
            tableNamingQuotes.getOrElse(engine.toString, tableNamingQuotes(Engine.JDBC.toString))
          (quote, separator)
        case (None, Some(separator)) =>
          val (quote, ignoreSeparator) =
            tableNamingQuotes.getOrElse(engine.toString, tableNamingQuotes(Engine.JDBC.toString))
          (quote, separator)
        case (None, None) =>
          tableNamingQuotes.getOrElse(engine.toString, tableNamingQuotes(Engine.JDBC.toString))
      }

    if (database.isEmpty) {
      if (domain.isEmpty) {
        table
      } else {
        s"$quote$domain$quote.$quote$table$quote"
      }
    } else {
      s"$quote$database$quote$separator$quote$domain$quote.$quote$table$quote"
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

case class RefDesc(version: Int, refs: List[Ref]) {
  @JsonCreator
  def this() =
    this(latestSchemaVersion, Nil) // Should never be called. Here for Jackson deserialization only

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

case class EnvDesc(
  version: Int,
  env: Map[String, String]
) {
  @JsonCreator
  def this() = this(
    latestSchemaVersion,
    Map.empty
  ) // Should never be called. Here for Jackson deserialization only

  override def toString: String = {
    val redactEnv = Utils.redact(env)
    s"Env($redactEnv)"
  }
}

object EnvDesc extends StrictLogging {
  def apply(env: Map[String, String]): EnvDesc = EnvDesc(latestSchemaVersion, env)
  def loadEnv(path: Path)(implicit storageHandler: StorageHandler): Option[EnvDesc] =
    if (storageHandler.exists(path)) {
      val envDesc = Option(
        YamlSerde.deserializeYamlEnvConfig(storageHandler.read(path), path.toString)
      )
      envDesc.map { envDesc =>
        if (!envDesc.env.contains("_updated")) {
          envDesc.copy(env = envDesc.env + ("_updated" -> LocalDateTime.now().toString))
        } else
          envDesc

      }
    } else {
      logger.warn(s"Env file $path not found")
      None
    }

  def checkValidity(implicit
    storageHandler: StorageHandler,
    settings: Settings
  ): List[ValidationMessage] = {
    var errors = List.empty[ValidationMessage]
    val defaultEnvPath = DatasetArea.env()
    val defaultEnv = loadEnv(defaultEnvPath).map(_.env).getOrElse(Map.empty)
    val allSpecificEnvs: List[(Path, Map[String, String])] =
      storageHandler
        .list(DatasetArea.metadata, extension = ".sl.yml", recursive = false)
        .filter { file =>
          val filename = file.path.getName()
          filename.startsWith("env.") && filename != "env.sl.yml"
        }
        .flatMap { file =>
          val path = file.path
          val env = loadEnv(path)
          env.map(e => (path, e.env))
        }
    val specificEnvVarsCount = allSpecificEnvs.map { case (path, vars) => vars.size }.sum
    if (defaultEnv.isEmpty && specificEnvVarsCount > 0) {
      val allSpecificFilenames =
        allSpecificEnvs.map { case (path, _) => path.getName() }.mkString(", ")
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "Env",
        s"Found Env specific $allSpecificFilenames. You cannot have env specific variables without the default env.sl.yml file"
      )
    }

    allSpecificEnvs.foreach { case (path, vars) =>
      val envVarsWithNoDefault = vars.keys.toSet -- defaultEnv.keys.toSet
      if (envVarsWithNoDefault.nonEmpty) {
        errors = errors :+ ValidationMessage(
          Severity.Warning,
          "Env",
          s"Specific env file ${path
              .getName()} has variables defined as empty strings: ${envVarsWithNoDefault.mkString(", ")}"
        )
      }
    }

    errors
  }

  def allEnvVars(implicit
    storageHandler: StorageHandler,
    settings: Settings
  ): Set[String] = {
    var errors = List.empty[ValidationMessage]
    val defaultEnvPath = DatasetArea.env()
    val defaultEnv = loadEnv(defaultEnvPath).map(_.env.keys).getOrElse(List.empty).toSet
    val allSpecificEnvVars =
      storageHandler
        .list(DatasetArea.metadata, extension = ".sl.yml", recursive = false)
        .filter { file =>
          val filename = file.path.getName()
          filename.startsWith("env.") && filename != "env.sl.yml"
        }
        .flatMap { file =>
          val path = file.path
          val env = loadEnv(path)
          env.map(e => e.env.keys)
        }
        .flatten
        .toSet
    allSpecificEnvVars ++ defaultEnv
  }
}
