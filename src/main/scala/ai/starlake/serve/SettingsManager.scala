package ai.starlake.serve

import ai.starlake.config.Settings

trait SettingsManager {
  def reset(): Boolean
  def uniqueId(
    root: String,
    env: Option[String],
    duckDbMode: Boolean
  ): String =
    root +
    "," + env.getOrElse("null") +
    "," + duckDbMode

  def getUpdatedSettings(
    root: String,
    env: Option[String],
    duckDbMode: Boolean,
    refresh: Boolean = false
  ): (Settings, Boolean)
}
