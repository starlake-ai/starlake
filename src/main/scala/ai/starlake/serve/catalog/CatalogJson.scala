package ai.starlake.serve.catalog

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.util.{Failure, Success, Try}

object CatalogJson {
  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def ok(data: Any): String =
    mapper.writeValueAsString(scala.collection.immutable.ListMap("ok" -> true, "data" -> data))

  def error(code: String, message: String): String =
    mapper.writeValueAsString(
      scala.collection.immutable.ListMap(
        "ok"    -> false,
        "error" -> scala.collection.immutable.ListMap("code" -> code, "message" -> message)
      )
    )

  def fromTry(result: Try[Any]): String = result match {
    case Success(data)                => ok(data)
    case Failure(e: CatalogException) => error(e.code, e.getMessage)
    case Failure(e)                   => error(CatalogErrorCode.QueryFailed, messageOf(e))
  }

  def messageOf(e: Throwable): String =
    Option(e.getMessage).filter(_.nonEmpty).getOrElse(e.getClass.getSimpleName)
}
