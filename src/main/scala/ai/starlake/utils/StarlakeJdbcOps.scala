package ai.starlake.utils

import com.typesafe.scalalogging.StrictLogging

import java.sql.Connection
import java.time.Instant

object StarlakeJdbcOps extends StrictLogging {

  case class BranchStatement(sql: String, ts: Instant)

  def isBranchActive(connection: Connection, branchName: Option[String]): Boolean = {
    val slConn = connection.asInstanceOf[ai.starlake.jdbc.StarlakeConnection]
    branchName match {
      case Some(b) =>
        slConn.currentSession != null && slConn.currentSession.getSessionId == b
      case None =>
        slConn.currentSession != null
    }
  }

  def currentBranch(connection: Connection): Option[String] = {
    val slConn = connection.asInstanceOf[ai.starlake.jdbc.StarlakeConnection]
    if (slConn.currentSession != null) Some(slConn.currentSession.getSessionId) else None
  }

  /** Start a branch if not already started
    * @param dataBranch
    *   the branch name
    * @param connection
    *   the JDBC connection
    * @return
    *   true if branch was not yet started, false if already on that branch
    */
  def branchStart(dataBranch: String, connection: Connection): Boolean = {
    // KEEP_SNAPSHOTS == KEEP survives session closure
    // KEEP_STATEMENTS ==  PERSIST means the sql statements are physically stored in the history table
    if (isBranchActive(connection, Some(dataBranch))) {
      logger.info(s"Already on branch $dataBranch")
      false
    } else {
      // We keep the sql history and the snapshot tables
      connection
        .createStatement()
        .execute(s"BRANCH START $dataBranch WITH PERSIST = TRUE, KEEP = TRUE ")
      true
    }
  }

  def branchTruncate(dataBranch: String, connection: Connection): Boolean = {
    // We keep the sql statements history but deletes all snapshot tables. Schema is kept but snapshots are removed
    connection.createStatement().execute(s"BRANCH DROP $dataBranch WITH KEEP = TRUE")
  }

  def branchDrop(dataBranch: String, connection: Connection): Boolean = {
    // Drop all the snapshot tables adn all teh sql statements history. Schema is removed thus all data is lost
    connection.createStatement().execute(s"BRANCH DROP $dataBranch")
  }

  def branchApply(dataBranch: String, connection: Connection): Boolean = {
    // run all the sql statements in history onto the production database
    connection.createStatement().execute(s"BRANCH APPLY $dataBranch")
  }

  def branchDescribe(dataBranch: String, connection: Connection): List[BranchStatement] = {
    // list all sql statements in the branch history
    val rs = connection.createStatement().executeQuery(s"BRANCH DESCRIBE $dataBranch")
    val result = scala.collection.mutable.ListBuffer[StarlakeJdbcOps.BranchStatement]()
    while (rs.next()) {
      val ts = rs.getObject(1).asInstanceOf[Instant]
      val sql = rs.getString(2)
      val item = StarlakeJdbcOps.BranchStatement(sql, ts)
      result += item
    }
    result.toList.sortBy(_.ts).reverse
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
