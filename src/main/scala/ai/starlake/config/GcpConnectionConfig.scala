package ai.starlake.config

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import org.apache.hadoop.fs.Path

trait GcpConnectionConfig {
  def gcpProjectId: Option[String]
  def gcpSAJsonKey: Option[String]
  def location: Option[String]

  def getLocation(): String = this.location.getOrElse("EU")

  def getCredentials()(implicit settings: Settings): Option[ServiceAccountCredentials] = {
    gcpSAJsonKey.map { gcpSAJsonKey =>
      val gcpSAJsonKeyAsString = if (!gcpSAJsonKey.trim.startsWith("{")) {
        val path = new Path(gcpSAJsonKey)
        if (settings.storageHandler.exists(path)) {
          settings.storageHandler.read(path)
        } else {
          throw new Exception(s"Invalid GCP SA KEY Path: $path")
        }
      } else
        gcpSAJsonKey
      val credentialsStream = new java.io.ByteArrayInputStream(
        gcpSAJsonKeyAsString.getBytes(java.nio.charset.StandardCharsets.UTF_8.name)
      )
      ServiceAccountCredentials.fromStream(credentialsStream)
    }
  }

  def bigquery()(implicit settings: Settings): BigQuery = {
    val bqOptionsBuilder = BigQueryOptions.newBuilder()
    val gcpDefaultProject = System.getProperty("GCP_PROJECT", System.getenv("GCP_PROJECT"))
    val projectId = gcpProjectId.orElse(scala.Option(gcpDefaultProject))
    val credentials = getCredentials()
    val bqOptions = projectId
      .map(bqOptionsBuilder.setProjectId)
      .getOrElse(bqOptionsBuilder)
    val bqService = credentials
      .map(bqOptions.setCredentials)
      .getOrElse(bqOptions)
      .build()
      .getService()
    bqService
  }

}
