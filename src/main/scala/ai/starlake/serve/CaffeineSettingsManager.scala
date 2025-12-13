package ai.starlake.serve

import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.config.{PrivacyLevels, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.ConnectionType
import ai.starlake.utils.Utils
import better.files.File
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.TimeUnit

class CaffeineSettingsManager extends SettingsManager with LazyLogging {
  val snowflakeTokenCache: Cache[String, String] = Caffeine
    .newBuilder()
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .maximumSize(10000)
    .build();

  val settingsCache: Cache[String, Settings] = Caffeine
    .newBuilder()
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .maximumSize(10000)
    .build();

  val schemaCache: Cache[String, SchemaHandler] = Caffeine
    .newBuilder()
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .maximumSize(10000)
    .build();

  val storageCache: Cache[String, StorageHandler] = Caffeine
    .newBuilder()
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .maximumSize(10000)
    .build();

  override def reset(): Boolean = {
    settingsCache.invalidateAll()
    true
  }

  override def remove(
    root: String,
    env: Option[String]
  ): Unit = {
    val sessionId = uniqueId(root, env)
    settingsCache.invalidate(sessionId)
    schemaCache.invalidate(sessionId + "_schema_handler")
    storageCache.invalidate(sessionId + "_storage_handler")
  }

  override def getUpdatedSettings(
    tenant: String,
    root: String, // contains project id and userid
    env: Option[String],
    refresh: Boolean,
    aesSecretKey: Option[String]
  ): (Settings, Boolean) = {
    val sessionId = uniqueId(root, env)
    Utils.resetJinjaClassLoader()
    PrivacyLevels.resetAllPrivacy()

    if (refresh) {
      this.remove(root, env)
    }

    val (settings, fromCache) =
      Option(settingsCache.getIfPresent(sessionId)) match {
        case Some(settings) =>
          logger.info(s"++++++++++++++ Loaded cached settings for env $env in $root")
          (settings, false)
        case None =>
          logger.info(s"--------------- new settings(tenant=$tenant, root=$root, env=$env)]")
          val updatedSession =
            Settings(Settings.referenceConfig, env, Some(root), aesSecretKey)

          settingsCache.put(sessionId, updatedSession)
          (updatedSession, true)
      }
    val connections = settings.appConfig.connections
    val connectionsWithSlDuckDB =
      connections.get("sl_duckdb") match {
        case Some(_) => connections
        case None =>
          File(root + "/datasets").createDirectories()
          val connectionsWithDuckDB = new ConnectionInfo(
            `type` = ConnectionType.JDBC,
            options = Map(
              "url"    -> s"jdbc:duckdb:$root/datasets/duckdb.db",
              "driver" -> "org.duckdb.DuckDBDriver"
            )
          )
          connections + ("sl_duckdb" -> connectionsWithDuckDB)
      }
    val apiConfigWithTenant =
      settings.appConfig.copy(tenant = tenant, connections = connectionsWithSlDuckDB)
    val updatedSettings = settings.copy(appConfig = apiConfigWithTenant)
    settingsCache.put(sessionId, updatedSettings)
    schemaCache.put(
      sessionId + "_schema_handler",
      updatedSettings.schemaHandler()
    )
    (updatedSettings, fromCache)
  }

  override def setSettings(
    root: String,
    env: Option[String],
    settings: Settings
  ): Unit = {
    val sessionId = uniqueId(root, env)
    settingsCache.put(sessionId, settings)
  }

  override def getSchemaHandler(
    root: String,
    env: Option[String]
  ): Option[SchemaHandler] = {
    val sessionId = uniqueId(root, env) + "_schema_handler"
    Option(schemaCache.getIfPresent(sessionId))
  }

  override def setSchemaHandler(
    root: String,
    env: Option[String],
    schemaHandler: SchemaHandler
  ): Unit = {
    val sessionId = uniqueId(root, env) + "_schema_handler"
    schemaCache.put(sessionId, schemaHandler)
  }
  override def getStorageHandler(
    root: String,
    env: Option[String]
  ): Option[StorageHandler] = {
    val sessionId = uniqueId(root, env) + "_storage_handler"
    Option(storageCache.getIfPresent(sessionId))
  }

  override def setStorageHandler(
    root: String,
    env: Option[String],
    storageHandler: StorageHandler
  ): Unit = {
    val sessionId = uniqueId(root, env) + "_storage_handler"
    storageCache.put(sessionId, storageHandler)
  }

  def setSnowflakeUserToken(
    user: String,
    token: String
  ): Unit =
    snowflakeTokenCache.put(user, token)

  def getSnowflakeUserToken(user: String): Option[String] =
    Option(snowflakeTokenCache.getIfPresent(user))

}

object CaffeineSettingsManager extends CaffeineSettingsManager
