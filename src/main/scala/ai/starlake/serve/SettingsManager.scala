package ai.starlake.serve

import ai.starlake.config.Settings

trait SettingsManager {
  def reset(): Boolean
  def uniqueId(
    root: String,
    env: Option[String]
  ): String =
    root +
    "," + env.getOrElse("null")
  def getUpdatedSettings(
    tenant: String,
    root: String,
    env: Option[String],
    refresh: Boolean = false
  ): (Settings, Boolean)

  def remove(
    root: String,
    env: Option[String]
  ): Unit

  def set(
    root: String,
    env: Option[String],
    settings: Settings
  ): Unit
}
