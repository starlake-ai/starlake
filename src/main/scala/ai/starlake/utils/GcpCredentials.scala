package ai.starlake.utils

import ai.starlake.job.sink.bigquery.BigQueryJobBase.{getJsonKeyStream, logger}
import com.google.auth.Credentials
import com.google.auth.oauth2.{
  AccessToken,
  GoogleCredentials,
  ServiceAccountCredentials,
  UserCredentials
}

import com.typesafe.scalalogging.StrictLogging

import scala.util.{Failure, Success, Try}

object GcpCredentials extends StrictLogging {
  def credentials(
    connectionOptions: Map[String, String],
    accessToken: scala.Option[String] = None
  ): scala.Option[Credentials] = {
    accessToken match {
      case Some(token) =>
        logger.info(s"Using inline access token credentials")
        val cred = GoogleCredentials.create(new AccessToken(token, null))
        scala.Option(cred)
      case None =>
        logger.info(s"Using ${connectionOptions("authType")} credentials")
        connectionOptions("authType") match {
          case "APPLICATION_DEFAULT" =>
            val refreshToken =
              Try(connectionOptions.getOrElse("refreshToken", "true").toBoolean).getOrElse(true)
            if (refreshToken) {
              val scopes = connectionOptions
                .getOrElse("authScopes", "https://www.googleapis.com/auth/cloud-platform")
                .split(',')
              val cred = GoogleCredentials.getApplicationDefault().createScoped(scopes: _*)
              Try {
                cred.refresh()
              } match {
                case Failure(e) =>
                  logger.warn(s"Error refreshing credentials: ${e.getMessage}")
                  None
                case Success(_) =>
                  Some(cred)
              }
            } else {
              scala.Option(GoogleCredentials.getApplicationDefault())
            }
          case "SERVICE_ACCOUNT_JSON_KEYFILE" =>
            val credentialsStream = getJsonKeyStream(connectionOptions)
            scala.Option(ServiceAccountCredentials.fromStream(credentialsStream))

          case "USER_CREDENTIALS" =>
            val clientId = connectionOptions("clientId")
            val clientSecret = connectionOptions("clientSecret")
            val refreshToken = connectionOptions("refreshToken")
            val cred = UserCredentials
              .newBuilder()
              .setClientId(clientId)
              .setClientSecret(clientSecret)
              .setRefreshToken(refreshToken)
              .build()
            scala.Option(cred)

          case "ACCESS_TOKEN" =>
            val accessToken = connectionOptions("gcpAccessToken")
            val cred = GoogleCredentials.create(new AccessToken(accessToken, null))
            scala.Option(cred)
        }
    }
  }
}
