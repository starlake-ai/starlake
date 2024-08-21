package ai.starlake.serve

import ai.starlake.config.Settings

trait SettingsManager {
  def reset(): Boolean
  def uniqueId(
    root: String,
    metadata: Option[String],
    env: Option[String],
    duckDbMode: Boolean
  ): String =
    root +
    "," + metadata.getOrElse("null") +
    "," + env.getOrElse("null") +
    "," + duckDbMode

  def getUpdatedSettings(
    root: String,
    metadata: Option[String],
    env: Option[String],
    gcpProject: Option[String],
    duckDbMode: Boolean,
    refresh: Boolean = false
  ): (Settings, Boolean)
}
