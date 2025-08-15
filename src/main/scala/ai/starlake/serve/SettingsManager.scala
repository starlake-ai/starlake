package ai.starlake.serve

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}

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
    refresh: Boolean,
    aesSecretKey: Option[String]
  ): (Settings, Boolean)

  def remove(
    root: String,
    env: Option[String]
  ): Unit

  def setSettings(
    root: String,
    env: Option[String],
    settings: Settings
  ): Unit

  def getSchemaHandler(
    root: String,
    env: Option[String]
  ): Option[SchemaHandler]

  def setSchemaHandler(
    root: String,
    env: Option[String],
    schemaHandler: SchemaHandler
  ): Unit
  def getStorageHandler(
    root: String,
    env: Option[String]
  ): Option[StorageHandler]

  def setStorageHandler(
    root: String,
    env: Option[String],
    storageHandler: StorageHandler
  ): Unit

}
