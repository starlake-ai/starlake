package ai.starlake.job.ingest.strategies

class RedshiftStrategiesBuilder extends JdbcStrategiesBuilder {
  override protected def createTemporaryView(viewName: String): String =
    s"CREATE OR REPLACE VIEW #$viewName"

  override protected def tempViewName(name: String) = s"#$name"

}
