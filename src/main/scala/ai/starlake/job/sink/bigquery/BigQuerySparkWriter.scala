package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.schema.model.{BigQuerySink, EsSink, FsSink, NoneSink, WriteMode}
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame

object BigQuerySparkWriter extends StrictLogging {
  def sink(
    authInfo: Map[String, String],
    df: DataFrame,
    tableName: String,
    maybeTableDescription: Option[String],
    writeMode: WriteMode
  )(implicit
    settings: Settings
  ): Any = {
    settings.comet.tableInfo.sink match {
      case sink: BigQuerySink =>
        val source = Right(Utils.setNullableStateOfColumn(df, nullable = true))
        val (createDisposition, writeDisposition) = {
          Utils.getDBDisposition(writeMode, hasMergeKeyDefined = false)
        }
        val bqLoadConfig =
          BigQueryLoadConfig(
            authInfo.get("gcpProjectId"),
            authInfo.get("gcpSAJsonKey"),
            source = source,
            outputTable = tableName,
            outputDataset = sink.name.getOrElse("audit"),
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
            acl = Nil
          )
        val result = new BigQuerySparkJob(
          bqLoadConfig,
          maybeSchema = None,
          maybeTableDescription = maybeTableDescription
        ).run()
        Utils.logFailure(result, logger)
        result.isSuccess
      case _: EsSink =>
        // TODO Sink Audit Log to ES
        throw new Exception("Sinking Audit log to Elasticsearch not yet supported")
      case _: NoneSink | FsSink(_, _, _, _, _, _) =>
      // Do nothing dataset already sinked to file. Forced at the reference.conf level
      case _ =>
    }
  }
}
