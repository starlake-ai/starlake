package ai.starlake.job

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.{Failure, Success, Try}

case class Tuple2Config[A, B](a: Option[A], b: Option[B])
trait Tuple2Cmd[A, B] extends Cmd[Tuple2Config[A, B]] {

  def a: Cmd[A]

  def b: Cmd[B]

  override def parse(args: Seq[String]): Option[Tuple2Config[A, B]] = {
    Some(Tuple2Config(a.parse(args), b.parse(args)))
  }

  override def parser: OParser[Unit, Tuple2Config[A, B]] = {
    val builder = OParser.builder[Tuple2Config[A, B]]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note("")
    )
  }

  override def run(config: Tuple2Config[A, B], schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val results =
      List(config.a.map(a.run(_, schemaHandler)), config.b.map(b.run(_, schemaHandler))).flatten
    results.find(_.isFailure) match {
      case Some(Failure(e)) =>
        Failure(e)
      case _ =>
        Success(JobResult.empty)
    }
  }
}
