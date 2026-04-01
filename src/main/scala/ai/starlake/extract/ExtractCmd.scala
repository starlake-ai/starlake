package ai.starlake.extract

import ai.starlake.job.{Cmd, Tuple2Cmd}

object ExtractCmd extends Tuple2Cmd[ExtractSchemaConfig, UserExtractDataConfig] {

  override def command: String = "extract"

  override def pageDescription: String =
    "Top-level extract command that groups sub-commands for extracting data, schemas, scripts, and BigQuery metadata."
  override def pageKeywords: Seq[String] =
    Seq("starlake extract", "data extraction", "schema extraction", "metadata extraction")

  override def a: Cmd[ExtractSchemaConfig] = ExtractSchemaCmd

  override def b: Cmd[UserExtractDataConfig] = ExtractDataCmd

}
