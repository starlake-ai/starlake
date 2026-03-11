package ai.starlake.job.gizmo

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.StrictLogging

import java.io.{BufferedReader, InputStreamReader, OutputStreamWriter}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}

class GizmoProcessClient(baseUrl: String, apiKey: Option[String] = None) extends StrictLogging {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  private def toJson(value: Any): String = mapper.writeValueAsString(value)
  private def fromJson[T](json: String, clazz: Class[T]): T = mapper.readValue(json, clazz)

  private val baseUri = if (baseUrl.endsWith("/")) baseUrl.dropRight(1) else baseUrl

  private def request(
    method: String,
    path: String,
    body: Option[String] = None
  ): Either[GizmoErrorResponse, String] = {
    val url = new URL(s"$baseUri/api/process/$path")
    logger.info(s"Gizmo $method $url")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod(method)
      conn.setRequestProperty("Content-Type", "application/json")
      conn.setRequestProperty("Accept", "application/json")
      apiKey.foreach(k => conn.setRequestProperty("X-API-Key", k))
      conn.setConnectTimeout(30000)
      conn.setReadTimeout(60000)

      body.foreach { b =>
        conn.setDoOutput(true)
        val writer = new OutputStreamWriter(conn.getOutputStream, StandardCharsets.UTF_8)
        try {
          writer.write(b)
          writer.flush()
        } finally {
          writer.close()
        }
      }

      val responseCode = conn.getResponseCode
      val stream =
        if (responseCode >= 200 && responseCode < 300) conn.getInputStream
        else conn.getErrorStream

      if (stream == null) {
        if (responseCode >= 200 && responseCode < 300) Right("")
        else Left(GizmoErrorResponse(s"HTTP $responseCode (no response body)"))
      } else {
        val reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
        try {
          val sb = new StringBuilder
          var line = reader.readLine()
          while (line != null) {
            sb.append(line)
            line = reader.readLine()
          }
          val responseBody = sb.toString()
          if (responseCode >= 200 && responseCode < 300) Right(responseBody)
          else Left(tryParseError(responseBody, responseCode))
        } finally {
          reader.close()
        }
      }
    } finally {
      conn.disconnect()
    }
  }

  private def tryParseError(body: String, code: Int): GizmoErrorResponse = {
    Try(fromJson(body, classOf[GizmoErrorResponse])) match {
      case Success(err) => err
      case Failure(_)   => GizmoErrorResponse(s"HTTP $code: $body")
    }
  }

  def startProcess(
    req: StartProcessRequest
  ): Either[GizmoErrorResponse, StartProcessResponse] = {
    request("POST", "start", Some(toJson(req))).map { body =>
      fromJson(body, classOf[StartProcessResponse])
    }
  }

  def stopProcess(
    req: StopProcessRequest
  ): Either[GizmoErrorResponse, StopProcessResponse] = {
    request("POST", "stop", Some(toJson(req))).map { body =>
      fromJson(body, classOf[StopProcessResponse])
    }
  }

  def restartProcess(
    req: RestartProcessRequest
  ): Either[GizmoErrorResponse, RestartProcessResponse] = {
    request("POST", "restart", Some(toJson(req))).map { body =>
      fromJson(body, classOf[RestartProcessResponse])
    }
  }

  def listProcesses(): Either[GizmoErrorResponse, ListProcessesResponse] = {
    request("GET", "list").map { body =>
      fromJson(body, classOf[ListProcessesResponse])
    }
  }

  def stopAll(): Either[GizmoErrorResponse, StopProcessResponse] = {
    request("POST", "stopAll").map { body =>
      fromJson(body, classOf[StopProcessResponse])
    }
  }
}
