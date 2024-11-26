package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import better.files.File
import com.google.cloud.MonitoredResource
import com.google.cloud.logging.Payload.JsonPayload
import com.google.cloud.logging.{LogEntry, LoggingOptions}

import java.util.{Collections, Locale}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

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

  def adapt_map_to_gcp_log(log: Map[String, Any]): Map[String, Any] = {
    // JsonPayload.of doesn't handle all types. Types handle by this method are defined in com.google.cloud.Structs.objectToValue.

    def adapt_values(value: Any): Any = {
      if (value != null) {
        value match {
          case v: Map[_, _] =>
            // force key to be string and value to any supported type
            v.iterator.map { case (k, v) =>
              (if (k != null) k.toString else k.asInstanceOf[String]) -> adapt_values(v)
            }.toMap
          case v: Iterable[_]                     => v.map(adapt_values)
          case _: String | _: Boolean | _: Number => value
          case _ if value.getClass.isEnum         => value
          // special handling starts here
          case v: java.sql.Timestamp => v.getTime
          case _ =>
            value.toString // by default, serialize to string instead of throwing an exception
        }
      } else {
        value
      }
    }
    adapt_values(log) match {
      case result: Map[String @unchecked, Any @unchecked] => result
      case result =>
        throw new RuntimeException(
          s"Expected type Map[String, Any], got ${result.getClass.getSimpleName}"
        )
    }
  }

  def sinkToGcpCloudLogging(log: Map[String, Any], typ: String, logName: String)(implicit
    settings: Settings
  ): Unit = {
    val connProjectId = settings.appConfig.audit.getSink().getConnection().options.get("projectId")
    val logging = LoggingOptions.getDefaultInstance
      .toBuilder()
      .setProjectId(BigQueryJobBase.projectId(connProjectId, settings.appConfig.audit.database))
      .build()
      .getService
    try {
      val entry = LogEntry
        .newBuilder(JsonPayload.of(adapt_map_to_gcp_log(log).asJava))
        .setSeverity(com.google.cloud.logging.Severity.INFO)
        .addLabel("type", typ)
        .addLabel("app", "starlake")
        .setLogName(logName)
        .setResource(MonitoredResource.newBuilder("global").build)
        .build
      // Writes the log entry asynchronously
      logging.write(Collections.singleton(entry))
      // Optional - flush any pending log entries just before Logging is closed
      BigQueryJobBase.recoverBigqueryException(logging.flush()) match {
        case Failure(exception) => throw exception
        case Success(_)         => //
      }
    } finally if (logging != null) logging.close()
  }

}
