package ai.starlake.job.transform

sealed abstract class TransformAction(val value: String) {
  override def toString: String = value
}

object TransformAction {

  def fromString(value: String): TransformAction = {
    value.toLowerCase() match {
      case "run"    => Run
      case "create" => Create
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid action '$value'. Valid values are: run, create"
        )
    }
  }

  final object Run extends TransformAction("run")

  final object Create extends TransformAction("create")
}
