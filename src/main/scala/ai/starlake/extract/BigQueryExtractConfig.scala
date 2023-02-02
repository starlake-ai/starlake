package ai.starlake.extract

import ai.starlake.config.GcpConnectionConfig

case class BigQueryExtractConfig(
  gcpProjectId: Option[String],
  gcpSAJsonKey: Option[String],
  location: Option[String] = None
) extends GcpConnectionConfig
