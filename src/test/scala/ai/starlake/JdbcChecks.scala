package ai.starlake

import java.sql.{DriverManager, ResultSet, SQLException, Timestamp}
import java.time.Instant

import ai.starlake.ResultSetScala.toResultSetExtra
import ai.starlake.config.Settings
import ai.starlake.job.ingest._
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

  val TestStart: Timestamp = Timestamp.from(Instant.now)

  protected def expectingJdbcDataset[T: ItemStandardizer](
    jdbcName: String,
    referenceDatasetName: String,
    columns: immutable.Seq[String],
    expectedValues: immutable.Seq[T]
  )(rowToEntity: ResultSet => T)(implicit settings: Settings): Assertion = {

    val jdbcOptions = settings.comet.connections(jdbcName)
    val engine = settings.comet.jdbcEngines(jdbcOptions.engine)

    val conn = DriverManager.getConnection(
      jdbcOptions.options("url"),
      jdbcOptions.options("user"),
      jdbcOptions.options("password")
    )
    try {
      val lacksTheTable =
        /* lacks the table, and not https://www.ikea.com/us/en/p/lack-side-table-white-30449908/ */
        try {
          val canary = conn.createStatement()
          canary.executeQuery(s"select * from ${referenceDatasetName} where 1=0").close()
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
            stmt.executeQuery(
              s"select ${columns.mkString(", ")} from ${referenceDatasetName}".stripMargin
            )

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

      val result = fetched
        .map(standardize.andThen(_.toString))
        .asInstanceOf[Seq[T]]

      val expected = expectedValues.map(standardize.andThen(_.toString))

      result should contain allElementsOf expected
    } finally {
      conn.close()
    }
  }

  implicit object ContinuousMetricRecordStandardizer
      extends ItemStandardizer[ContinuousMetricRecord] {

    override def standardize(item: ContinuousMetricRecord): ContinuousMetricRecord = {
      item.copy(cometTime = 0L, jobId = "")
    }
  }

  implicit object DiscreteMetricRecordStandardizer extends ItemStandardizer[DiscreteMetricRecord] {

    override def standardize(item: DiscreteMetricRecord): DiscreteMetricRecord = {
      item.copy(cometTime = 0L, jobId = "")
    }
  }

  implicit object FrequencyMetricRecordStandardizer
      extends ItemStandardizer[FrequencyMetricRecord] {
    private val FakeDuration = Random.nextInt(5000)

    override def standardize(item: FrequencyMetricRecord): FrequencyMetricRecord = {
      item.copy(cometTime = 0L, jobId = "")
      // We pretend the AuditLog entry has been generated exactly at TestStart.
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
      "duration" :: "message" :: "step" :: Nil,
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
        rs.getString("message"),
        rs.getString("step")
      )

      item.timestamp.after(TestStart) should be(true)
      item.timestamp.before(testEnd) should be(true)

      item
    }
  }

  protected def expectingMetrics(
    jdbcName: String,
    continuous: Seq[ContinuousMetricRecord],
    discrete: Seq[DiscreteMetricRecord],
    frequencies: Seq[FrequencyMetricRecord]
  )(implicit
    settings: Settings
  ): Assertion = {
    val testEnd: Timestamp = Timestamp.from(Instant.now)

    expectingJdbcDataset(
      jdbcName,
      "continuous",
      "domain" :: "schema" :: "attribute" ::
      "min" :: "max" :: "mean" :: "missingValues" :: "standardDev" :: "variance" :: "sum" ::
      "skewness" :: "kurtosis" :: "percentile25" :: "median" :: "percentile75" ::
      "count" :: "cometTime" :: "cometStage" :: "cometMetric" :: "jobId" :: Nil,
      continuous.to[Vector]
    ) { rs =>
      ContinuousMetricRecord(
        domain = rs.getString("domain"),
        schema = rs.getString("schema"),
        attribute = rs.getString("attribute"),
        min = rs.getDoubleOption("min"),
        max = rs.getDoubleOption("max"),
        mean = rs.getDoubleOption("mean"),
        missingValues = rs.getLongOption("missingValues"),
        standardDev = rs.getDoubleOption("standardDev"),
        variance = rs.getDoubleOption("variance"),
        sum = rs.getDoubleOption("sum"),
        skewness = rs.getDoubleOption("skewness"),
        kurtosis = rs.getDoubleOption("kurtosis"),
        percentile25 = rs.getDoubleOption("percentile25"),
        median = rs.getDoubleOption("median"),
        percentile75 = rs.getDoubleOption("percentile75"),
        count = rs.getLong("count"),
        cometTime = 0L, // Do not include time since iwe are in test mode
        cometStage = rs.getString("cometStage"),
        cometMetric = rs.getString("cometMetric"),
        jobId = "" // Do not include jobId in test mode
      )
    }

    expectingJdbcDataset(
      jdbcName,
      "discrete",
      "domain" :: "schema" :: "attribute" ::
      "missingValuesDiscrete" :: "countDistinct" :: "count" ::
      "cometTime" :: "cometStage" :: "cometMetric" :: "jobId" :: Nil,
      discrete.to[Vector]
    ) { rs =>
      DiscreteMetricRecord(
        domain = rs.getString("domain"),
        schema = rs.getString("schema"),
        attribute = rs.getString("attribute"),
        missingValuesDiscrete = rs.getLong("missingValuesDiscrete"),
        countDistinct = rs.getLong("countDistinct"),
        count = rs.getLong("count"),
        cometTime = rs.getLong("cometTime"),
        cometStage = rs.getString("cometStage"),
        cometMetric = rs.getString("cometMetric"),
        jobId = rs.getString("jobId")
      )
    }

    expectingJdbcDataset(
      jdbcName,
      "frequencies",
      "domain" :: "schema" :: "attribute" ::
      "category" :: "frequency" :: "count" ::
      "cometTime" :: "cometStage" :: "jobId" :: Nil,
      frequencies.to[Vector]
    ) { rs =>
      FrequencyMetricRecord(
        domain = rs.getString("domain"),
        schema = rs.getString("schema"),
        attribute = rs.getString("attribute"),
        category = rs.getString("category"),
        frequency = rs.getLong("frequency"),
        count = rs.getLong("count"),
        cometTime = rs.getLong("cometTime"),
        cometStage = rs.getString("cometStage"),
        jobId = rs.getString("jobId")
      )
    }
  }
}

trait ItemStandardizer[T] {
  def standardize(value: T): T
}

trait ItemStandardizerLowPriority {

  implicit def identityStandardizer[T]: ItemStandardizer[T] =
    new ItemStandardizerLowPriority.IdentityStandardizer[T]
}

object ItemStandardizerLowPriority {

  final class IdentityStandardizer[T]() extends ItemStandardizer[T] {
    override def standardize(value: T): T = value
  }
}

object ItemStandardizer extends ItemStandardizerLowPriority {}
