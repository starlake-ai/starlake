package ai.starlake.job.ingest

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.sink.bigquery.BigQueryJobResult
import ai.starlake.job.transform.SparkAutoTask
import ai.starlake.privacy.PrivacyEngine
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.Rejection.{ColInfo, ColResult}
import ai.starlake.schema.model.Trim.{BOTH, LEFT, NONE, RIGHT}
import ai.starlake.schema.model._
import com.google.cloud.bigquery.{Field, LegacySQLTypeName, Schema => BQSchema}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}
object IngestionUtil {

  private val rejectedCols = List(
    ("jobid", LegacySQLTypeName.STRING, StringType),
    ("timestamp", LegacySQLTypeName.TIMESTAMP, TimestampType),
    ("domain", LegacySQLTypeName.STRING, StringType),
    ("schema", LegacySQLTypeName.STRING, StringType),
    ("error", LegacySQLTypeName.STRING, StringType),
    ("path", LegacySQLTypeName.STRING, StringType)
  )

  private def bigqueryRejectedSchema(): BQSchema = {
    val fields = rejectedCols map { case (attrName, attrLegacyType, attrStandardType) =>
      Field
        .newBuilder(attrName, attrLegacyType)
        .setMode(Field.Mode.NULLABLE)
        .setDescription("")
        .build()
    }
    BQSchema.of(fields: _*)
  }

  def sinkRejected(
    session: SparkSession,
    rejectedDS: Dataset[String],
    domainName: String,
    schemaName: String,
    now: Timestamp
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[(Dataset[Row], Path)] = {
    import session.implicits._
    val rejectedPath = new Path(DatasetArea.rejected(domainName), schemaName)
    val rejectedPathName = rejectedPath.toString
    // We need to save first the application ID
    // referencing it inside the worker (rdd.map) below would fail.
    val applicationId = session.sparkContext.applicationId
    val rejectedTypedDS = rejectedDS.map { err =>
      RejectedRecord(
        applicationId,
        now,
        domainName,
        schemaName,
        err,
        rejectedPathName
      )
    }
    val rejectedDF = rejectedTypedDS
      .limit(settings.appConfig.audit.maxErrors)
      .toDF(rejectedCols.map { case (attrName, _, _) => attrName }: _*)

    val taskDesc =
      AutoTaskDesc(
        name = s"rejected-$applicationId-$domainName-$schemaName",
        sql = None,
        database = settings.appConfig.audit.getDatabase(),
        domain = settings.appConfig.audit.getDomain(),
        table = "rejected",
        write = Some(WriteMode.APPEND),
        partition = Nil,
        presql = Nil,
        postsql = Nil,
        sink = Some(settings.appConfig.audit.sink),
        _auditTableName = Some("rejected")
      )
    val autoTask = new SparkAutoTask(
      taskDesc,
      Map.empty,
      None,
      truncate = false
    )
    val res = autoTask.sink(rejectedDF)
    if (res) {
      Success(rejectedDF, rejectedPath)
    } else {
      Failure(new Exception("Failed to save rejected"))
    }
  }

  def validateCol(
    colRawValue: Option[String],
    colAttribute: Attribute,
    tpe: Type,
    colMap: => Map[String, Option[String]],
    allPrivacyLevels: Map[String, ((PrivacyEngine, List[String]), PrivacyLevel)],
    emptyIsNull: Boolean
  ): ColResult = {
    def ltrim(s: String) = s.replaceAll("^\\s+", "")

    def rtrim(s: String) = s.replaceAll("\\s+$", "")

    val trimmedColValue = colRawValue.map { colRawValue =>
      colAttribute.trim match {
        case Some(NONE) | None => colRawValue
        case Some(LEFT)        => ltrim(colRawValue)
        case Some(RIGHT)       => rtrim(colRawValue)
        case Some(BOTH)        => colRawValue.trim()
        case _                 => colRawValue
      }
    }

    val colValue = colAttribute.default match {
      case None =>
        trimmedColValue
      case Some(default) =>
        trimmedColValue
          .map(value => if (value.isEmpty) default else value)
          .orElse(colAttribute.default)
    }

    def colValueIsNullOrEmpty = colValue match {
      case None           => true
      case Some(colValue) => colValue.isEmpty
    }

    def colValueIsNull = colValue.isEmpty

    def optionalColIsEmpty = !colAttribute.required && colValueIsNullOrEmpty

    def requiredColIsEmpty = {
      if (emptyIsNull)
        colAttribute.required && colValueIsNullOrEmpty
      else
        colAttribute.required && colValueIsNull
    }

    def colPatternIsValid = colValue.exists(tpe.matches)

    val privacyLevel = colAttribute.getPrivacy()
    val colValueWithPrivacyApplied =
      if (privacyLevel == PrivacyLevel.None || privacyLevel.sql) {
        colValue
      } else {
        val ((privacyAlgo, privacyParams), _) = allPrivacyLevels(privacyLevel.value)
        colValue.map(colValue => privacyLevel.crypt(colValue, colMap, privacyAlgo, privacyParams))

      }

    val colPatternOK = !requiredColIsEmpty && (optionalColIsEmpty || colPatternIsValid)

    val (sparkValue, colParseOK) = {
      (colPatternOK, colValueWithPrivacyApplied) match {
        case (true, Some(colValueWithPrivacyApplied)) =>
          Try(tpe.sparkValue(colValueWithPrivacyApplied)) match {
            case Success(res) => (Some(res), true)
            case Failure(_)   => (None, false)
          }
        case (colPatternResult, _) =>
          (None, colPatternResult)
      }
    }
    ColResult(
      ColInfo(
        colValue,
        colAttribute.name,
        tpe.name,
        tpe.pattern,
        colPatternOK && colParseOK
      ),
      sparkValue.orNull
    )
  }
}

case class BqLoadInfo(
  totalAcceptedRows: Long,
  totalRejectedRows: Long,
  jobResult: BigQueryJobResult
) {
  val totalRows: Long = totalAcceptedRows + totalRejectedRows
}
