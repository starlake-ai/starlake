package ai.starlake.schema.model

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import ai.starlake.schema.model.Severity._

case class Freshness(
  warn: Option[String] = None,
  error: Option[String] = None
) {
  def checkValidity(): Either[List[ValidationMessage], Boolean] = {
    val errorList: mutable.MutableList[ValidationMessage] = mutable.MutableList.empty

    def checkDuration(duration: Option[String]): Unit = {
      Try {
        duration.map(Duration(_).toSeconds)
      } match {
        case Failure(_) =>
          errorList += ValidationMessage(
            Error,
            "Freshness",
            s"duration: $duration could not be parsed as a duration"
          )
        case Success(_) =>
      }
    }

    checkDuration(warn)
    checkDuration(error)
    if (errorList.isEmpty)
      Right(true)
    else
      Left(errorList.toList)
  }
}
