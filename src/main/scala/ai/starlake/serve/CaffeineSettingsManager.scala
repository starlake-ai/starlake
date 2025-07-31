package ai.starlake.serve

import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.config.{PrivacyLevels, Settings}
import ai.starlake.schema.model.ConnectionType
import ai.starlake.utils.Utils
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}

import java.util.concurrent.TimeUnit

class CaffeineSettingsManager extends SettingsManager {
  val cache: Cache[String, Settings] = Caffeine
    .newBuilder()
    .expireAfterAccess(5, TimeUnit.MINUTES)
    .maximumSize(10000)
    .build();

  override def reset(): Boolean = {
    cache.invalidateAll()
    true
  }

  override def remove(
    root: String,
    env: Option[String]
  ): Unit = {
    val sessionId = uniqueId(root, env)
    cache.invalidate(sessionId)
  }

  override def getUpdatedSettings(
    tenant: String,
    root: String, // contains project id and userid
    env: Option[String],
    refresh: Boolean
  ): (Settings, Boolean) = {
    val sessionId = uniqueId(root, env)
    Utils.resetJinjaClassLoader()
    PrivacyLevels.resetAllPrivacy()

    if (refresh) {
      cache.invalidate(sessionId)
    }

    val (settings, fromCache) =
      Option(cache.getIfPresent(sessionId)) match {
        case Some(settings) =>
          (settings, false)
        case None =>
          val updatedSession = {
            println("new settings")
            Settings(Settings.referenceConfig, env, Some(root))
          }
          cache.put(sessionId, updatedSession)
          (updatedSession, true)
      }
    val connections = settings.appConfig.connections
    val connectionsWithSlDuckDB =
      connections.get("sl_duckdb") match {
        case Some(_) => connections
        case None =>
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
    (settings.copy(appConfig = apiConfigWithTenant), fromCache)
  }

  override def set(
    root: String,
    env: Option[String],
    settings: Settings
  ): Unit = {
    val sessionId = uniqueId(root, env)
    cache.put(sessionId, settings)
  }
}

object CaffeineSettingsManager extends CaffeineSettingsManager
