package ai.starlake.job.transform

case class TransformTestConfig(accessToken: Option[String] = None) {
  def toArgs: Array[String] = {
    val accessToken =
      this.accessToken.map(x => Array("--accessToken", x)).getOrElse(Array.empty[String])
    accessToken
  }
}
