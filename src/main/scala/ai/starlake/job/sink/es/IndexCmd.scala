package ai.starlake.job.sink.es

object IndexCmd extends ESLoadCmd {
  override def command: String = "index"
}
