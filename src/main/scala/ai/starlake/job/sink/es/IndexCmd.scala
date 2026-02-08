package ai.starlake.job.sink.es

/** Command to index data in Elasticsearch (alias for esload).
  *
  * Usage: starlake index [options]
  */
object IndexCmd extends ESLoadCmd {
  override def command: String = "index"
}
