package ai.starlake.job.sink.es

/** Command to index data in Elasticsearch (alias for esload).
  *
  * Usage: starlake index [options]
  */
object IndexCmd extends ESLoadCmd {
  override def command: String = "index"

  override def pageDescription: String =
    "Alias for esload. Load datasets into Elasticsearch indices."
  override def pageKeywords: Seq[String] =
    Seq("starlake index", "Elasticsearch", "index loading")
}
