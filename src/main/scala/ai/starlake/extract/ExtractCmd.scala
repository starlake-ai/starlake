package ai.starlake.extract

import ai.starlake.job.{Cmd, Tuple2Cmd}

object ExtractCmd extends Tuple2Cmd[ExtractSchemaConfig, UserExtractDataConfig] {

  override def command: String = "extract"

  override def a: Cmd[ExtractSchemaConfig] = ExtractJDBCSchemaCmd

  override def b: Cmd[UserExtractDataConfig] = ExtractDataCmd

}
