package ai.starlake.job.sink.es

object IndexCmd extends ESLoadCmd {
  override val command: String = "index"
}
