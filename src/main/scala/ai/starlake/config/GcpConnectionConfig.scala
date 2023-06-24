package ai.starlake.config

trait GcpConnectionConfig {
  def gcpProjectId: Option[String]
  def gcpSAJsonKey: Option[String]
  def location: Option[String]

  def getLocation(): String = this.location.getOrElse("EU")

  def authInfo(): Map[String, String] = {
    val authInfo = scala.collection.mutable.Map[String, String]()
    gcpProjectId match {
      case Some(prj) => authInfo += ("gcpProjectId" -> prj)
      case None      =>
    }
    gcpSAJsonKey match {
      case Some(prj) => authInfo += ("gcpSAJsonKey" -> prj)
      case None      =>
    }
    authInfo.toMap
  }
}
