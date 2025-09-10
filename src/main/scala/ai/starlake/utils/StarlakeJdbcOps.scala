package ai.starlake.utils

import com.typesafe.scalalogging.StrictLogging

import java.sql.Connection

object StarlakeJdbcOps extends StrictLogging {

  def branchStart(dataBranch: String, connection: Connection) = {
    connection.createStatement().execute(s"BRANCH START $dataBranch WITH PERSIST = TRUE")
  }

  def branchTruncate(dataBranch: String, connection: Connection) = {
    connection.createStatement().execute(s"BRANCH DROP $dataBranch WITH KEEP = TRUE")
  }

  def branchDrop(dataBranch: String, connection: Connection) = {
    connection.createStatement().execute(s"BRANCH DROP $dataBranch")
  }

  def branchApply(dataBranch: String, connection: Connection) = {
    connection.createStatement().execute(s"BRANCH APPLY $dataBranch")
  }

  def branchDescribe(dataBranch: String, connection: Connection): String = {
    val rs = connection.createStatement().executeQuery(s"BRANCH DESCRIBE $dataBranch")
    if (rs.next()) {
      rs.getString(1)
    } else {
      "No branch"
    }
  }

  def driverAndUrl(dataBranch: Option[String], driver: String, url: String): (String, String) = {
    dataBranch match {
      case Some(_) =>
        val engine = url.split(':')(1).toLowerCase()
        engine match {
          case "snowflake" =>
            val finalDriver = "ai.starlake.jdbc.StarlakeDriver"
            val finalUrl = "jdbc:starlake:" + url.substring("jdbc:".length)
            logger.info(s"Using StarlakeDriver for Snowflake: $finalUrl")
            (finalDriver, finalUrl)
          case _ => (driver, url)
        }
      case None => (driver, url)
    }
  }
}
