package com.ebiznext.comet.job.ingest

import java.sql.Timestamp

case class RejectedRecord(
  jobid: String,
  timestamp: Timestamp,
  domain: String,
  schema: String,
  error: String,
  path: String
)
