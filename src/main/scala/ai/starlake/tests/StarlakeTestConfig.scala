package ai.starlake.tests

case class StarlakeTestConfig(
  accessToken: Option[String] = None,
  load: Boolean = false,
  transform: Boolean = false,
  name: Option[String] = None
) {
  def runLoad(): Boolean = load || (!load && !transform)

  def runTransform(): Boolean = transform || (!load && !transform)

  def toArgs: Array[String] = {
    val accessToken =
      this.accessToken.map(x => Array("--accessToken", x)).getOrElse(Array.empty[String])
    accessToken
  }
}
