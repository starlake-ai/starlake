package ai.starlake.serve

import ai.starlake.config.Settings
import better.files.File
import com.typesafe.config.ConfigFactory

class SettingsWatcherThread(
  settingsMap: scala.collection.mutable.Map[String, Settings],
  settingsTimeMap: scala.collection.mutable.Map[String, Long]
) extends Thread {
  private val ONE_MINUTE = 1000 * 60
  private val TEN_MINUTES = ONE_MINUTE * 10
  override def run() {
    while (true) {
      Thread.sleep(ONE_MINUTE)
      val currentTime = System.currentTimeMillis()
      for ((key, time) <- settingsTimeMap) {
        if (currentTime - time > TEN_MINUTES) {
          settingsMap.synchronized {
            settingsMap.remove(key)
          }
        }
      }
    }
  }
}

object SettingsManager {
  private val settingsMap: scala.collection.mutable.Map[String, Settings] =
    scala.collection.mutable.Map.empty

  private val settingsTimeMap: scala.collection.mutable.Map[String, Long] =
    scala.collection.mutable.Map.empty

  private val watcherThread = new SettingsWatcherThread(settingsMap, settingsTimeMap)
  watcherThread.start()

  private def uniqueId(
    root: String,
    metadata: Option[String],
    env: Option[String]
  ): String = root +
    "," + metadata.getOrElse("null") +
    "," + env.getOrElse("null")

  def getUpdatedSettings(
    root: String,
    metadata: Option[String],
    env: Option[String],
    gcpProject: Option[String]
  ): Settings = {
    val sessionId = uniqueId(root, metadata, env)

    val sysProps = System.getProperties()
    val outFile = File(root, "out")
    outFile.createDirectoryIfNotExists()

    gcpProject.foreach { gcpProject =>
      sysProps.setProperty("SL_DATABASE", gcpProject)
    }
    val currentSettings = settingsMap.getOrElse(
      sessionId, {
        settingsMap.synchronized {
          sysProps.setProperty("root-serve", outFile.pathAsString)
          sysProps.setProperty("root", root)
          sysProps.setProperty("metadata", root + "/" + metadata.getOrElse("metadata"))

          env match {
            case Some(env) if env.nonEmpty && env != "None" =>
              sysProps.setProperty("env", env)
            case _ =>
              sysProps.setProperty("env", "prod") // prod is the default value in reference.conf
          }
          ConfigFactory.invalidateCaches()
          val settings = Settings(ConfigFactory.load())
          settingsMap.put(sessionId, settings)
          settings
        }
      }
    )
    settingsTimeMap.put(sessionId, System.currentTimeMillis())
    currentSettings
  }
}
