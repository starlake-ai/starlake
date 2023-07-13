package ai.starlake.schema.model

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

case class Freshness(
  partitionFilter: Option[String] = None,
  timestamp: Option[String] = None,
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
    timestamp match {
      case Some(_) if warn.isEmpty && error.isEmpty =>
        errorList += ValidationMessage(
          Error,
          "freshness",
          "timestamp: When freshness timestamp is present, warn and/or error duration should be defined"
        )
      case _ =>
    }
    if (errorList.isEmpty)
      Right(true)
    else
      Left(errorList.toList)
  }
}
