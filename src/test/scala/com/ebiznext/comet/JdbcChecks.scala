package com.ebiznext.comet

import java.sql.{DriverManager, ResultSet, SQLException, Timestamp}
import java.time.Instant

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.ingest.{AuditLog, MetricRecord, RejectedRecord}
import org.scalatest.Assertion

import scala.annotation.tailrec
import scala.collection.immutable
import scala.language.implicitConversions
import scala.util.Random

object ResultSetScala {

  case class ResultSetExtra(rs: ResultSet) extends AnyVal {

    private def getFooOption[T](columnIndex: Int, getFoo: Int => T): Option[T] = {
      val l = getFoo(columnIndex)
      if (rs.wasNull()) None else Some(l)
    }

    private def getFooOption[T](columnLabel: String, getFoo: String => T): Option[T] = {
      val l = getFoo(columnLabel)
      if (rs.wasNull()) None else Some(l)
    }

    def getLongOption(columnLabel: String): Option[Long] =
      getFooOption(columnLabel, rs.getLong(_: String))

    def getLongOption(columnIndex: Int): Option[Long] =
      getFooOption(columnIndex, rs.getLong(_: Int))

    def getDoubleOption(columnLabel: String): Option[Double] =
      getFooOption(columnLabel, rs.getDouble(_: String))

    def getDoubleOption(columnIndex: Int): Option[Double] =
      getFooOption(columnIndex, rs.getDouble(_: Int))

    def getStringOption(columnLabel: String): Option[String] = Option(rs.getString(columnLabel))
    def getStringOption(columnIndex: Int): Option[String] = Option(rs.getString(columnIndex))
  }

  implicit def toResultSetExtra(rs: ResultSet): ResultSetExtra = ResultSetExtra(rs)
}

trait JdbcChecks {
  this: TestHelper =>
  import ResultSetScala._

  val TestStart: Timestamp = Timestamp.from(Instant.now)

  protected def expectingJdbcDataset[T: ItemStandardizer](
    jdbcName: String,
    referenceDatasetName: String,
    columns: immutable.Seq[String],
    expectedValues: immutable.Seq[T]
  )(rowToEntity: ResultSet => T)(implicit settings: Settings): Assertion = {

    val jdbcOptions = settings.comet.jdbc(jdbcName)
    val engine = settings.comet.jdbcEngines(jdbcOptions.engine)

    val conn = DriverManager.getConnection(jdbcOptions.uri, jdbcOptions.user, jdbcOptions.password)
    try {
      val tableName = engine.tables(referenceDatasetName).name

      val lacksTheTable =
        /* lacks the table, and not https://www.ikea.com/us/en/p/lack-side-table-white-30449908/ */
        try {
          val canary = conn.createStatement()
          canary.executeQuery(s"select * from ${tableName} where 1=0").close()
          None
        } catch {
          case ex: SQLException =>
            /* this is okay! we're almost certainly lacking a table, which is as good as empty for this purpose */
            Some(Vector.empty)
        }

      val fetched = lacksTheTable.getOrElse {
        /* We've validated that the table exists. So now we must succeed in pulling data from it. */
        val stmt = conn.createStatement()

        val fetched: Vector[T] = {
          val rs =
            stmt.executeQuery(s"select ${columns.mkString(", ")} from ${tableName}".stripMargin)

          @tailrec
          def pull(base: Vector[T]): Vector[T] = {
            if (!rs.next()) {
              base
            } else {
              val item = rowToEntity(rs)
              pull(base :+ item)
            }
          }

          pull(Vector.empty)
        }
        fetched
      }

      val standardize = implicitly[ItemStandardizer[T]].standardize _

      fetched.map(standardize) should contain theSameElementsInOrderAs (expectedValues.map(
        standardize
      ))
    } finally {
      conn.close()
    }
  }

  implicit object AuditLogStandardizer extends ItemStandardizer[AuditLog] {
    private val FakeDuration = Random.nextInt(5000)

    override def standardize(item: AuditLog): AuditLog = {
      item.copy(
        timestamp = TestStart,
        duration = FakeDuration
      ) // We pretend the AuditLog entry has been generated exactly at TestStart.
    }
  }

  implicit object RejectedRecordStandardizer extends ItemStandardizer[RejectedRecord] {

    override def standardize(item: RejectedRecord): RejectedRecord = {
      item.copy(timestamp =
        TestStart
      ) // We pretend the RejectedRecord entry has been generated exactly at TestStart.
    }
  }

  protected def expectingRejections(jdbcName: String, values: RejectedRecord*)(implicit
    settings: Settings
  ): Assertion = {
    val testEnd: Timestamp = Timestamp.from(Instant.now)

    expectingJdbcDataset(
      jdbcName,
      "rejected",
      "jobid" :: "timestamp" :: "domain" :: "schema" :: "error" :: "path" :: Nil,
      values.to[Vector]
    ) { rs =>
      val item = RejectedRecord(
        rs.getString("jobid"),
        rs.getTimestamp("timestamp"),
        rs.getString("domain"),
        rs.getString("schema"),
        rs.getString("error"),
        rs.getString("path")
      )

      item.timestamp.after(TestStart) should be(true)
      item.timestamp.before(testEnd) should be(true)

      item
    }

  }

  protected def expectingAudit(jdbcName: String, values: AuditLog*)(implicit
    settings: Settings
  ): Assertion = {
    val testEnd: Timestamp = Timestamp.from(Instant.now)

    expectingJdbcDataset(
      jdbcName,
      "audit",
      "jobid" :: "paths" :: "domain" :: "schema" :: "success" ::
      "count" :: "countAccepted" :: "countRejected" :: "timestamp" ::
      "duration" :: "message" :: Nil,
      values.to[Vector]
    ) { rs =>
      val item = AuditLog(
        rs.getString("jobid"),
        rs.getString("paths"),
        rs.getString("domain"),
        rs.getString("schema"),
        rs.getBoolean("success"),
        rs.getInt("count"),
        rs.getInt("countAccepted"),
        rs.getInt("countRejected"),
        rs.getTimestamp("timestamp"),
        rs.getInt("duration"),
        rs.getString("message")
      )

      item.timestamp.after(TestStart) should be(true)
      item.timestamp.before(testEnd) should be(true)

      item
    }
  }

  protected def expectingMetrics(jdbcName: String, values: MetricRecord*)(implicit
    settings: Settings
  ): Assertion = {
    val testEnd: Timestamp = Timestamp.from(Instant.now)

    val converter = MetricRecord.MetricRecordConverter()

    expectingJdbcDataset(
      jdbcName,
      "metrics",
      "domain" :: "schema" ::
      "min" :: "max" :: "mean" :: "missingValues" :: "standardDev" :: "variance" :: "sum" ::
      "skewness" :: "kurtosis" :: "percentile25" :: "median" :: "percentile75" ::
      "countDistinct" :: "catCountFreq" :: "missingValuesDiscrete" :: "count" ::
      "cometTime" :: "cometStage" :: Nil,
      values.to[Vector]
    ) { rs =>
      val itemAsSql = MetricRecord.AsSql(
        rs.getString("domain"),
        rs.getString("schema"),
        rs.getString("attribute"),
        rs.getLongOption("min"),
        rs.getLongOption("max"),
        rs.getDoubleOption("mean"),
        rs.getLongOption("missingValues"),
        rs.getDoubleOption("standardDev"),
        rs.getDoubleOption("variance"),
        rs.getLongOption("sum"),
        rs.getDoubleOption("skewness"),
        rs.getLongOption("kurtosis"),
        rs.getLongOption("percentile25"),
        rs.getLongOption("median"),
        rs.getLongOption("percentile75"),
        rs.getLongOption("countDistinct"),
        rs.getStringOption("catCountFreq"),
        rs.getLongOption("missingValuesDiscrete"),
        rs.getLong("count"),
        rs.getLong("cometTime"),
        rs.getString("cometStage")
      )

      val item = converter.fromSqlCompatible(itemAsSql)
      item
    }
  }

}

trait ItemStandardizer[T] {
  def standardize(value: T): T
}

trait ItemStandardizerLowPriority {

  implicit def identityStandardizer[T]: ItemStandardizer[T] =
    new ItemStandardizerLowPriority.IdentityStandardizer[T]()
}

object ItemStandardizerLowPriority {

  final class IdentityStandardizer[T]() extends ItemStandardizer[T] {
    override def standardize(value: T): T = value
  }
}

object ItemStandardizer extends ItemStandardizerLowPriority {}
