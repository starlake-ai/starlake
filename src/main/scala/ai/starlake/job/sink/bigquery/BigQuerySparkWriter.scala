package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.schema.model.{BigQuerySink, EsSink, FsSink, NoneSink, WriteMode}
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success}

object BigQuerySparkWriter extends StrictLogging {
  def sink(
    df: DataFrame,
    tableName: String,
    maybeTableDescription: Option[String],
    writeMode: WriteMode
  )(implicit
    settings: Settings
  ): Unit = {
    settings.comet.audit.sink match {
      case sink: BigQuerySink =>
        val source = Right(Utils.setNullableStateOfColumn(df, nullable = true))
        val (createDisposition, writeDisposition) = {
          Utils.getDBDisposition(writeMode, hasMergeKeyDefined = false)
        }
        val bqLoadConfig =
          BigQueryLoadConfig(
            connection = settings.comet.audit.sink.name,
            source = source,
            outputTableId = Some(
              BigQueryJobBase
                .extractProjectDatasetAndTable(
                  settings.comet.audit.database,
                  sink.name.getOrElse("audit"),
                  tableName
                )
            ),
            sourceFormat = settings.comet.defaultFormat,
            createDisposition = createDisposition,
            writeDisposition = writeDisposition,
            location = sink.location,
            outputPartition = sink.timestamp,
            outputClustering = sink.clustering.getOrElse(Nil),
            days = sink.days,
            requirePartitionFilter = sink.requirePartitionFilter.getOrElse(false),
            rls = Nil,
            options = sink.getOptions,
            acl = Nil,
            outputDatabase = settings.comet.audit.database
          )
        val result = new BigQuerySparkJob(
          bqLoadConfig,
          maybeSchema = None,
          maybeTableDescription = maybeTableDescription
        ).run()
        result match {
          case Success(_) => ;
          case Failure(e) =>
            throw e
        }
      case _: EsSink =>
        // TODO Sink Audit Log to ES
        throw new Exception("Sinking Audit log to Elasticsearch not yet supported")
      case _: NoneSink | FsSink(_, _, _, _, _, _, _) =>
      // Do nothing dataset already sinked to file. Forced at the reference.conf level
      case _ =>
    }
  }
}
