package ai.starlake.serve

import ai.starlake.config.{PrivacyLevels, Settings}
import ai.starlake.utils.Utils
import better.files.File

class SettingsWatcherThread(
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

object SettingsManager {
  private val settingsMap: scala.collection.concurrent.TrieMap[String, Settings] =
    scala.collection.concurrent.TrieMap.empty

  private val settingsTimeMap: scala.collection.concurrent.TrieMap[String, Long] =
    scala.collection.concurrent.TrieMap.empty

  private val watcherThread = new SettingsWatcherThread(settingsMap, settingsTimeMap)
  watcherThread.start()

  def reset(): Boolean = {
    lastSettingsId = ""
    settingsTimeMap.clear()
    settingsMap.clear()
    true
  }

  // Used in vscode plugin only
  var lastSettingsId: String = ""

  private def uniqueId(
    root: String,
    metadata: Option[String],
    env: Option[String],
    duckDbMode: Boolean
  ): String = root +
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
  ): (Settings, Boolean) = {
    val sessionId = uniqueId(root, metadata, env, duckDbMode)
    Utils.resetJinjaClassLoader()
    PrivacyLevels.resetAllPrivacy()

    val sysProps = System.getProperties()
    val outFile = File(root, "output")
    outFile.createDirectoryIfNotExists()
    val extensionFile = File(root, "extension.log")
    if (extensionFile.exists) {
      extensionFile.delete(swallowIOExceptions = true)
    }

    gcpProject.foreach { gcpProject =>
      sysProps.setProperty("database", gcpProject)
    }
    if (refresh) {
      settingsMap.remove(sessionId)
    }
    val updatedSession = settingsMap.getOrElseUpdate(
      sessionId, {
        sysProps.setProperty("root", root)
        sysProps.setProperty("SL_ROOT", root)
        sysProps.setProperty("metadata", root + "/" + metadata.getOrElse("metadata"))

        env match {
          case None =>
            sysProps.setProperty("env", "None")
          case Some(env) if env.isEmpty || env == "None" =>
            sysProps.setProperty("env", "None")
          case Some(env) =>
            sysProps.setProperty("env", env) // prod is the default value in reference.conf
        }
        Settings.invalidateCaches()
        println("new settings")
        Settings(Settings.referenceConfig)
      }
    )
    settingsTimeMap.put(sessionId, System.currentTimeMillis())
    val isNew = sessionId != lastSettingsId
    lastSettingsId = sessionId
    (updatedSession, isNew)
  }
}
