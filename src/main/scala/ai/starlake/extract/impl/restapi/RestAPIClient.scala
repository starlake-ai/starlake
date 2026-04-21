package ai.starlake.extract.impl.restapi

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging

import java.io.{BufferedReader, InputStreamReader, OutputStreamWriter}
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import java.util.Base64

/** HTTP response wrapper */
case class RestAPIResponse(
  statusCode: Int,
  body: JsonNode,
  headers: Map[String, String]
)

/** HTTP client for REST API extraction. Handles authentication, rate limiting, retries, and JSON
  * parsing.
  */
class RestAPIClient(
  config: RestAPIExtractSchema
) extends LazyLogging {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  @volatile private var oauth2Token: Option[String] = None
  @volatile private var oauth2Expiry: Long = 0L

  private val rateLimitDelayMs: Long =
    config.rateLimit.map(rl => 1000L / rl.requestsPerSecond).getOrElse(0L)

  @volatile private var lastRequestTime: Long = 0L

  /** Execute a GET or POST request to the given URL with query parameters and headers. Includes
    * retry logic for transient failures.
    */
  def execute(
    path: String,
    method: RestAPIHttpMethod = RestGet,
    queryParams: Map[String, String] = Map.empty,
    extraHeaders: Map[String, String] = Map.empty,
    requestBody: Option[String] = None,
    maxRetries: Int = 3
  ): RestAPIResponse = {
    throttle()
    val fullUrl = buildUrl(path, queryParams)
    var lastException: Exception = null
    var attempt = 0
    var oauth2TokenRefreshed = false

    while (attempt <= maxRetries) {
      try {
        val response = doRequest(fullUrl, method, extraHeaders, requestBody)
        if (
          response.statusCode == 401 && config.auth.exists(
            _.isInstanceOf[OAuth2ClientCredentials]
          ) && !oauth2TokenRefreshed
        ) {
          // Token may have expired mid-session — invalidate and retry once
          logger.info(s"HTTP 401 for $fullUrl, refreshing OAuth2 token and retrying")
          oauth2Token = None
          oauth2Expiry = 0L
          oauth2TokenRefreshed = true
          // Don't increment attempt — this is a token refresh, not a retry
        } else if (response.statusCode == 429 || response.statusCode >= 500) {
          if (attempt < maxRetries) {
            val backoffMs = math.min(1000L * math.pow(2, attempt).toLong, 30000L)
            logger.warn(
              s"HTTP ${response.statusCode} for $fullUrl, retrying in ${backoffMs}ms (attempt ${attempt + 1}/$maxRetries)"
            )
            Thread.sleep(backoffMs)
            attempt += 1
          } else {
            throw new RestAPIException(
              s"HTTP ${response.statusCode} after $maxRetries retries for $fullUrl"
            )
          }
        } else if (response.statusCode >= 400) {
          throw new RestAPIException(
            s"HTTP ${response.statusCode} for $fullUrl: ${response.body}"
          )
        } else {
          return response
        }
      } catch {
        case e: RestAPIException => throw e
        case e: Exception =>
          lastException = e
          if (attempt < maxRetries) {
            val backoffMs = math.min(1000L * math.pow(2, attempt).toLong, 30000L)
            logger.warn(s"Request to $fullUrl failed: ${e.getMessage}, retrying in ${backoffMs}ms")
            Thread.sleep(backoffMs)
            attempt += 1
          } else {
            throw new RestAPIException(
              s"Request to $fullUrl failed after $maxRetries retries: ${e.getMessage}",
              lastException
            )
          }
      }
    }
    throw new RestAPIException(
      s"Request to $fullUrl failed after $maxRetries retries",
      lastException
    )
  }

  /** Fetch all pages from an endpoint, returning an iterator of (dataRecords, responseHeaders) */
  def fetchAllPages(
    endpoint: RestAPIEndpoint,
    additionalParams: Map[String, String] = Map.empty
  ): Iterator[(JsonNode, Map[String, String])] = {
    val paginationHandler = endpoint.pagination
      .map(PaginationHandler.apply)
      .getOrElse(PaginationHandler.noPagination)

    new Iterator[(JsonNode, Map[String, String])] {
      private var currentParams: Option[Map[String, String]] =
        Some(additionalParams ++ paginationHandler.initialParams)
      private var pageIndex: Int = 0
      private var nextResult: Option[(JsonNode, Map[String, String])] = None
      private var done: Boolean = false

      fetchNext()

      private def fetchNext(): Unit = {
        if (done) {
          nextResult = None
          return
        }
        currentParams match {
          case None =>
            nextResult = None
            done = true
          case Some(params) =>
            val allParams = endpoint.queryParams ++ params
            val response = execute(
              endpoint.path,
              endpoint.method,
              allParams,
              endpoint.headers,
              endpoint.requestBody
            )
            val dataNode = JsonPathUtil.extractDataArray(response.body, endpoint.responsePath)

            // Check if we got any data
            val hasData = dataNode.isArray && dataNode.size() > 0 ||
              !dataNode.isArray && !dataNode.isNull && !dataNode.isMissingNode

            if (!hasData) {
              nextResult = None
              done = true
            } else {
              nextResult = Some((dataNode, response.headers))
              // Pass both full response (for cursor paths) and data node (for size checks)
              currentParams = paginationHandler.nextPageParams(
                response.body,
                dataNode,
                response.headers,
                pageIndex
              )
              pageIndex += 1
              if (currentParams.isEmpty) {
                done = true // This was the last page
              }
            }
        }
      }

      override def hasNext: Boolean = nextResult.isDefined

      override def next(): (JsonNode, Map[String, String]) = {
        val result =
          nextResult.getOrElse(throw new NoSuchElementException("No more pages"))
        fetchNext()
        result
      }
    }
  }

  private def throttle(): Unit = {
    if (rateLimitDelayMs > 0) {
      val now = System.currentTimeMillis()
      val elapsed = now - lastRequestTime
      if (elapsed < rateLimitDelayMs) {
        Thread.sleep(rateLimitDelayMs - elapsed)
      }
      lastRequestTime = System.currentTimeMillis()
    }
  }

  private def buildUrl(path: String, queryParams: Map[String, String]): String = {
    val baseUrl = config.baseUrl.stripSuffix("/")
    val cleanPath = if (path.startsWith("/")) path else s"/$path"
    val url = s"$baseUrl$cleanPath"
    if (queryParams.isEmpty) url
    else {
      val qs = queryParams
        .map { case (k, v) =>
          s"${URLEncoder.encode(k, "UTF-8")}=${URLEncoder.encode(v, "UTF-8")}"
        }
        .mkString("&")
      s"$url?$qs"
    }
  }

  private def doRequest(
    fullUrl: String,
    method: RestAPIHttpMethod,
    extraHeaders: Map[String, String],
    requestBody: Option[String]
  ): RestAPIResponse = {
    val url = new URL(fullUrl)
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod(method match {
        case RestGet  => "GET"
        case RestPost => "POST"
      })
      conn.setRequestProperty("Accept", "application/json")
      conn.setConnectTimeout(30000)
      conn.setReadTimeout(60000)

      // Apply global headers
      config.headers.foreach { case (k, v) => conn.setRequestProperty(k, v) }
      // Apply extra headers (endpoint-specific)
      extraHeaders.foreach { case (k, v) => conn.setRequestProperty(k, v) }
      // Apply authentication
      applyAuth(conn)

      // Send body if POST
      requestBody.foreach { body =>
        conn.setDoOutput(true)
        conn.setRequestProperty("Content-Type", "application/json")
        val writer = new OutputStreamWriter(conn.getOutputStream, StandardCharsets.UTF_8)
        try {
          writer.write(body)
          writer.flush()
        } finally {
          writer.close()
        }
      }

      val responseCode = conn.getResponseCode
      val responseHeaders = extractResponseHeaders(conn)
      val stream =
        if (responseCode >= 200 && responseCode < 300) conn.getInputStream
        else conn.getErrorStream

      val bodyStr = if (stream == null) "" else readStream(stream)
      val contentType = responseHeaders.getOrElse("content-type", "application/json")
      val bodyNode =
        if (bodyStr.isEmpty) mapper.createObjectNode()
        else if (contentType.contains("xml")) XmlToJsonConverter.convert(bodyStr, mapper)
        else mapper.readTree(bodyStr)

      RestAPIResponse(responseCode, bodyNode, responseHeaders)
    } finally {
      conn.disconnect()
    }
  }

  private def applyAuth(conn: HttpURLConnection): Unit = {
    config.auth.foreach {
      case BearerAuth(token) =>
        conn.setRequestProperty("Authorization", s"Bearer ${resolveEnvVar(token)}")

      case ApiKeyAuth(key, header) =>
        conn.setRequestProperty(header, resolveEnvVar(key))

      case BasicAuth(username, password) =>
        val credentials =
          Base64.getEncoder.encodeToString(
            s"${resolveEnvVar(username)}:${resolveEnvVar(password)}".getBytes(
              StandardCharsets.UTF_8
            )
          )
        conn.setRequestProperty("Authorization", s"Basic $credentials")

      case oauth: OAuth2ClientCredentials =>
        val token = getOAuth2Token(oauth)
        conn.setRequestProperty("Authorization", s"Bearer $token")
    }
  }

  private def getOAuth2Token(oauth: OAuth2ClientCredentials): String = {
    val now = System.currentTimeMillis()
    if (oauth2Token.isDefined && now < oauth2Expiry) {
      return oauth2Token.get
    }
    // Fetch new token
    val url = new URL(resolveEnvVar(oauth.tokenUrl))
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod("POST")
      conn.setDoOutput(true)
      conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
      conn.setConnectTimeout(15000)
      conn.setReadTimeout(15000)

      val params = scala.collection.mutable.ListBuffer(
        s"grant_type=client_credentials",
        s"client_id=${URLEncoder.encode(resolveEnvVar(oauth.clientId), "UTF-8")}",
        s"client_secret=${URLEncoder.encode(resolveEnvVar(oauth.clientSecret), "UTF-8")}"
      )
      oauth.scope.foreach(s => params += s"scope=${URLEncoder.encode(resolveEnvVar(s), "UTF-8")}")

      val writer = new OutputStreamWriter(conn.getOutputStream, StandardCharsets.UTF_8)
      try {
        writer.write(params.mkString("&"))
        writer.flush()
      } finally {
        writer.close()
      }

      val responseCode = conn.getResponseCode
      if (responseCode != 200) {
        throw new RestAPIException(
          s"OAuth2 token request failed with HTTP $responseCode"
        )
      }

      val bodyStr = readStream(conn.getInputStream)
      val bodyNode = mapper.readTree(bodyStr)
      val token = bodyNode.get("access_token").asText()
      val expiresIn = if (bodyNode.has("expires_in")) bodyNode.get("expires_in").asLong() else 3600L

      oauth2Token = Some(token)
      oauth2Expiry = now + (expiresIn * 1000L) - 60000L // Refresh 1 minute early
      token
    } finally {
      conn.disconnect()
    }
  }

  private def extractResponseHeaders(conn: HttpURLConnection): Map[String, String] = {
    val headers = scala.collection.mutable.Map[String, String]()
    var i = 0
    var headerName = conn.getHeaderFieldKey(i)
    while (headerName != null || i == 0) {
      if (headerName != null) {
        headers += (headerName.toLowerCase -> conn.getHeaderField(i))
      }
      i += 1
      headerName = conn.getHeaderFieldKey(i)
    }
    headers.toMap
  }

  private def readStream(stream: java.io.InputStream): String = {
    val reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
    try {
      val sb = new StringBuilder
      var line = reader.readLine()
      while (line != null) {
        sb.append(line)
        line = reader.readLine()
      }
      sb.toString()
    } finally {
      reader.close()
    }
  }

  /** Resolve environment variable references like {{VAR_NAME}}.
    *
    * Note: When configs are loaded from YAML via CLI commands, Starlake's Formatter.richFormat
    * already resolves {{VAR}} patterns before deserialization, so values typically arrive
    * pre-resolved. This method serves as a fallback for programmatic usage.
    */
  private[restapi] def resolveEnvVar(value: String): String = {
    val pattern = """\{\{([^}]+)\}\}""".r
    pattern.replaceAllIn(
      value,
      m => {
        val envVar = m.group(1).trim
        sys.env.getOrElse(
          envVar,
          throw new RestAPIException(s"Environment variable '$envVar' not set")
        )
      }
    )
  }
}

class RestAPIException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
