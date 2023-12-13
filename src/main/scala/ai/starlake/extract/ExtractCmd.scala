package ai.starlake.extract

import ai.starlake.job.{Cmd, Tuple2Cmd}

object ExtractCmd extends Tuple2Cmd[ExtractSchemaConfig, ExtractDataConfig] {

  override def command: String = "extract"

  override def _1: Cmd[ExtractSchemaConfig] = ExtractJDBCSchemaCmd

  override def _2: Cmd[ExtractDataConfig] = ExtractDataCmd

}
