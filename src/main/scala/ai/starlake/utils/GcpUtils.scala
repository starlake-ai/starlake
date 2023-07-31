package ai.starlake.utils

import com.google.auth.oauth2.{GoogleCredentials, UserCredentials}

import java.io.{File, FileInputStream, IOException, InputStream}
import java.util.Locale

object GcpUtils {
  private val WELL_KNOWN_CREDENTIALS_FILE = "application_default_credentials.json"

  private val CLOUDSDK_CONFIG_DIRECTORY = "gcloud"

  def getDefaultCredentials() = {
    val cred = GoogleCredentials.getApplicationDefault()
    cred match {
      case cred: UserCredentials =>
        val userCred = cred.asInstanceOf[UserCredentials]
        (userCred.getClientId(), userCred.getClientSecret(), userCred.getRefreshToken())
    }

  }
  @throws[IOException]
  def getCredentialUsingWellKnownFile(): GoogleCredentials = {
    val wellKnownFileLocation = getWellKnownCredentialsFile()
    var credentialsStream: InputStream = null
    try {
      credentialsStream = new FileInputStream(wellKnownFileLocation)
      GoogleCredentials.fromStream(credentialsStream)
    } catch {
      case e: IOException =>
        throw new IOException(
          String.format(
            "Error reading credential file from location %s: %s",
            wellKnownFileLocation,
            e.getMessage
          )
        )
    } finally if (credentialsStream != null) credentialsStream.close()
  }

  private def getWellKnownCredentialsFile(): java.io.File = {
    var cloudConfigPath: File = null
    val os = System.getProperty("os.name", "").toLowerCase(Locale.US)
    if (os.indexOf("windows") >= 0) {
      val appDataPath = new File(System.getenv("APPDATA"))
      cloudConfigPath = new File(appDataPath, CLOUDSDK_CONFIG_DIRECTORY)
    } else {
      val configPath = new File(System.getProperty("user.home", ""), ".config")
      cloudConfigPath = new File(configPath, CLOUDSDK_CONFIG_DIRECTORY)
    }
    val credentialFilePath = new File(cloudConfigPath, WELL_KNOWN_CREDENTIALS_FILE)
    credentialFilePath
  }

}
