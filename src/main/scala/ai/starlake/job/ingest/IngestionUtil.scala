package ai.starlake.job.ingest

import ai.starlake.job.sink.bigquery.BigQueryJobResult

case class BqLoadInfo(
  totalAcceptedRows: Long,
  totalRejectedRows: Long,
  paths: List[String],
  jobResult: BigQueryJobResult
) {
  val totalRows: Long = totalAcceptedRows + totalRejectedRows
}
