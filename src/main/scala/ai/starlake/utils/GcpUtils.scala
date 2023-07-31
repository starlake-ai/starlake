package ai.starlake.utils

import better.files.File
import java.util.Locale

object GcpUtils {
  private val WELL_KNOWN_CREDENTIALS_FILE = "application_default_credentials.json"

  private val CLOUDSDK_CONFIG_DIRECTORY = "gcloud"

  def getApplicationDefaultFile(): Option[File] = {
    val credentialsPath = sys.env.get("GOOGLE_APPLICATION_CREDENTIALS")
    credentialsPath.map(File(_)).orElse {
      val applicationDefaultFile = getWellKnownCredentialsFile()
      applicationDefaultFile
    }
  }

  protected def getWellKnownCredentialsFile(): Option[File] = {
    val os = System.getProperty("os.name", "").toLowerCase(Locale.US)
    val cloudConfigPath = if (os.indexOf("windows") >= 0) {
      val appDataPath = File(System.getenv("APPDATA"))
      File(appDataPath, CLOUDSDK_CONFIG_DIRECTORY)
    } else {
      val configPath = File(System.getProperty("user.home", ""), ".config")
      File(configPath, CLOUDSDK_CONFIG_DIRECTORY)
    }
    val credentialFilePath = File(cloudConfigPath, WELL_KNOWN_CREDENTIALS_FILE)
    if (credentialFilePath.exists) {
      Some(credentialFilePath)
    } else {
      None
    }
  }
}
