package ai.starlake.utils

case class ProxySettings(host: String, port: Int, username: String, password: String) {
  def setJavaProxy(protocol: String): Unit = {
    if (host != null) {
      System.setProperty(protocol + ".proxyHost", host)
      System.setProperty(protocol + ".proxyPort", String.valueOf(port))
    }
    if (username != null) System.setProperty(protocol + ".proxyUser", username)
    if (password != null) System.setProperty(protocol + ".proxyPassword", password)
  }

}

object ProxySettings {
  private def parseProxy(proxyProtocol: String): ProxySettings = {
    if (proxyProtocol.contains("@")) {
      val last = proxyProtocol.lastIndexOf("@")
      val credentials = proxyProtocol.substring(0, last)
      val username = credentials.split(":")(1).substring(2)
      val password = credentials.split(":")(2)
      val hostAndPort = proxyProtocol.substring(last + 1)
      if (hostAndPort.contains(":")) {
        val host = hostAndPort.split(":")(0)
        val port = hostAndPort.split(":")(1).toInt
        ProxySettings(host, port, username, password)
      } else {
        val host = hostAndPort
        val port = if (proxyProtocol == "http") 80 else 443
        ProxySettings(host, port, username, password)
      }
    } else {
      val hostAndPort = proxyProtocol.split(":")(1).substring(2)
      if (hostAndPort.contains(":")) {
        val host = hostAndPort.split(":")(0)
        val port = hostAndPort.split(":")(1).toInt
        ProxySettings(host, port, null, null)
      } else {
        val host = hostAndPort
        val port = if (proxyProtocol == "http") 80 else 443
        ProxySettings(host, port, null, null)
      }
    }
  }

  def setProxy(): Unit = {
    val httpsProxyProperty = Option(System.getProperty("https.proxyHost"))
    val httpProxyProperty = Option(System.getProperty("http.proxyHost"))
    if (httpsProxyProperty.isEmpty && httpProxyProperty.isEmpty) {
      val httpsProxy = Option(System.getenv("https_proxy")).getOrElse("")
      val httpProxy = Option(System.getenv("http_proxy")).getOrElse("")
      val noProxy =
        Option(System.getenv("no_proxy")).getOrElse("").replaceAll(",", "|")

      if (httpsProxy.nonEmpty) {
        val proxySettings = parseProxy(httpsProxy)
        proxySettings.setJavaProxy("https")
      }
      if (httpProxy.nonEmpty) {
        val proxySettings = parseProxy(httpProxy)
        proxySettings.setJavaProxy("http")
      }
      if (noProxy.nonEmpty) System.setProperty("http.nonProxyHosts", noProxy)
    }
  }
}
