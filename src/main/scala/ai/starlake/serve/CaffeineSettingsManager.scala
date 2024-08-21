package ai.starlake.serve

import ai.starlake.config.{PrivacyLevels, Settings}
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

  override def getUpdatedSettings(
    root: String,
    metadata: Option[String],
    env: Option[String],
    gcpProject: Option[String],
    duckDbMode: Boolean,
    refresh: Boolean
  ): (Settings, Boolean) = {
    val sessionId = uniqueId(root, metadata, env, duckDbMode)
    Utils.resetJinjaClassLoader()
    PrivacyLevels.resetAllPrivacy()

    val sysProps = System.getProperties()
    gcpProject.foreach { gcpProject =>
      sysProps.setProperty("database", gcpProject)
    }
    if (refresh) {
      cache.invalidate(sessionId)
    }

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
  }
}
