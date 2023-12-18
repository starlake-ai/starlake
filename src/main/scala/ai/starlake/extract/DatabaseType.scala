package ai.starlake.extract

sealed trait DatabaseType
case object Mysql extends DatabaseType
case object Unknown extends DatabaseType

object DatabaseType {
  def detect(url: String): DatabaseType = {
    if (url.startsWith("jdbc:mysql:")) {
      Mysql
    } else {
      Unknown
    }
  }
}
