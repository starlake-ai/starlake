package ai.starlake.job

case class CheckValidityCommand(
  tables: Boolean = true,
  tasks: Boolean = true,
  reload: Boolean = true,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
