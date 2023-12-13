package ai.starlake.job

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.{Failure, Success, Try}

case class Tuple2Config[A, B](_1: Option[A], _2: Option[B]) extends Product2[Option[A], Option[B]]
trait Tuple2Cmd[A, B] extends Cmd[Tuple2Config[A, B]] with Product2[Cmd[A], Cmd[B]] {

  override def canEqual(that: Any): Boolean = {
    that match {
      case tuple: Tuple2Cmd[A, B] => tuple._1 == _1 && tuple._2 == _2
      case _                      => false
    }
  }

  override def parse(args: Seq[String]): Option[Tuple2Config[A, B]] = {
    Some(Tuple2Config(_1.parse(args), _2.parse(args)))
  }

  override def parser: OParser[Unit, Tuple2Config[A, B]] = {
    val builder = OParser.builder[Tuple2Config[A, B]]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note("")
    )
  }

  override def run(config: Tuple2Config[A, B], schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val results =
      List(config._1.map(_1.run(_, schemaHandler)), config._2.map(_2.run(_, schemaHandler))).flatten
    results.find(_.isFailure) match {
      case Some(Failure(e)) =>
        Failure(e)
      case _ =>
        Success(JobResult.empty)
    }
  }
}
