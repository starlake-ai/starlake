package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import better.files.File
import com.google.cloud.MonitoredResource
import com.google.cloud.logging.Payload.JsonPayload
import com.google.cloud.logging.{LogEntry, LoggingOptions}

import java.util.{Collections, Locale}
import scala.jdk.CollectionConverters._

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

  def sinkToGcpCloudLogging(log: Map[String, Any], typ: String, logName: String)(implicit
    settings: Settings
  ): Unit = {
    val logging = LoggingOptions.getDefaultInstance
      .toBuilder()
      .setProjectId(BigQueryJobBase.projectId(settings.appConfig.audit.database))
      .build()
      .getService
    try {
      val entry = LogEntry
        .newBuilder(JsonPayload.of(log.asJava))
        .setSeverity(com.google.cloud.logging.Severity.INFO)
        .addLabel("type", typ)
        .addLabel("app", "starlake")
        .setLogName(logName)
        .setResource(MonitoredResource.newBuilder("global").build)
        .build
      // Writes the log entry asynchronously
      logging.write(Collections.singleton(entry))
      // Optional - flush any pending log entries just before Logging is closed
      logging.flush()
    } finally if (logging != null) logging.close()
  }

}
