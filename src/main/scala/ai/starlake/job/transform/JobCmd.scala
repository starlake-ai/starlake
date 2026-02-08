package ai.starlake.job.transform

/** Command to run a job (alias for transform).
  *
  * Usage: starlake job [options]
  */
object JobCmd extends TransformCmd {
  override def command: String = "job"
}
