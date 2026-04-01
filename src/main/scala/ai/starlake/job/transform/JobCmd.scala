package ai.starlake.job.transform

/** Command to run a job (alias for transform).
  *
  * Usage: starlake job [options]
  */
object JobCmd extends TransformCmd {
  override def command: String = "job"

  override def pageDescription: String =
    "Execute a named Spark or BigQuery job with configurable options, parameters, and interactive output formats."
  override def pageKeywords: Seq[String] =
    Seq("starlake job", "job execution", "Spark job", "BigQuery job")
}
