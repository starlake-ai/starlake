package ai.starlake.tests

case class StarlakeTestConfig(accessToken: Option[String] = None) {
  def toArgs: Array[String] = {
    val accessToken =
      this.accessToken.map(x => Array("--accessToken", x)).getOrElse(Array.empty[String])
    accessToken
  }
}
