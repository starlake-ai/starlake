package ai.starlake.serve

import ai.starlake.config.{PrivacyLevels, Settings}
import ai.starlake.utils.Utils

class MapSettingsWatcherThread(
  settingsMap: scala.collection.mutable.Map[String, Settings],
  settingsTimeMap: scala.collection.mutable.Map[String, Long]
) extends Thread {
  private val ONE_MINUTE = 1000 * 60
  private val TEN_MINUTES = ONE_MINUTE * 10
  override def run(): Unit = {
    while (true) {
      Thread.sleep(ONE_MINUTE)
      val currentTime = System.currentTimeMillis()
      for ((key, time) <- settingsTimeMap.toSet) {
        if (currentTime - time > TEN_MINUTES) {
          settingsTimeMap.remove(key)
          settingsMap.remove(key)
        }
      }
    }
  }
}

class MapSettingsManager extends SettingsManager {
  private val settingsMap: scala.collection.concurrent.TrieMap[String, Settings] =
    scala.collection.concurrent.TrieMap.empty

  private val settingsTimeMap: scala.collection.concurrent.TrieMap[String, Long] =
    scala.collection.concurrent.TrieMap.empty

  private val watcherThread = new MapSettingsWatcherThread(settingsMap, settingsTimeMap)
  watcherThread.start()

  def reset(): Boolean = {
    lastSettingsId = ""
    settingsTimeMap.clear()
    settingsMap.clear()
    true
  }

  // Used in vscode plugin only
  var lastSettingsId: String = ""

  def getUpdatedSettings(
    root: String,
    env: Option[String],
    duckDbMode: Boolean,
    refresh: Boolean = false
  ): (Settings, Boolean) = {
    val sessionId = uniqueId(root, env, duckDbMode)
    Utils.resetJinjaClassLoader()
    PrivacyLevels.resetAllPrivacy()

    if (refresh) {
      settingsMap.remove(sessionId)
    }
    val updatedSession = settingsMap.getOrElseUpdate(
      sessionId, {
        println("new settings")
        Settings(Settings.referenceConfig, env, Some(root))
      }
    )
    settingsTimeMap.put(sessionId, System.currentTimeMillis())
    val isNew = sessionId != lastSettingsId
    lastSettingsId = sessionId
    (updatedSession, isNew)
  }
}

object MapSettingsManager extends MapSettingsManager
