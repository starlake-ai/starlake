package com.ebiznext.comet.job.jdbcload

import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import org.apache.spark.sql.DataFrame
import scopt.OParser

case class JdbcLoadConfig(
  sourceFile: Either[String, DataFrame] = Left(""),
  outputTable: String = "",
  createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED,
  writeDisposition: WriteDisposition = WriteDisposition.WRITE_APPEND,
  driver: String = "",
  url: String = "",
  user: String = "",
  password: String = "",
  partitions: Int = 1,
  batchSize: Int = 1000
)

object JdbcLoadConfig {

  // comet bqload  --source_file xxx --output_table schema --source_format parquet --create_disposition  CREATE_IF_NEEDED --write_disposition WRITE_TRUNCATE
  //               --partitions 1  --batch_size 1000 --user username --password pwd -- url jdbcurl
  def parse(args: Seq[String]): Option[JdbcLoadConfig] = {
    val builder = OParser.builder[JdbcLoadConfig]
    val parser: OParser[Unit, JdbcLoadConfig] = {
      import builder._
      OParser.sequence(
        programName("comet"),
        head("comet", "1.x"),
        opt[String]("source_file")
          .action((x, c) => c.copy(sourceFile = Left(x)))
          .text("Full Path to source file"),
        opt[String]("output_table")
          .action((x, c) => c.copy(outputTable = x))
          .text("JDBC Output Table"),
        opt[String]("driver")
          .action((x, c) => c.copy(driver = x))
          .text("JDBC Driver to use"),
        opt[String]("partitions")
          .action((x, c) => c.copy(partitions = x.toInt))
          .text("Number of Spark Partitions"),
        opt[String]("batch_size")
          .action((x, c) => c.copy(batchSize = x.toInt))
          .text("JDBC Batch Size"),
        opt[String]("user")
          .action((x, c) => c.copy(user = x))
          .text("JDBC user"),
        opt[String]("password")
          .action((x, c) => c.copy(password = x))
          .text("JDBC password"),
        opt[String]("url")
          .action((x, c) => c.copy(url = x))
          .text("Database JDBC URL"),
        opt[String]("create_disposition")
          .action((x, c) => c.copy(createDisposition = CreateDisposition.valueOf(x)))
          .text(
            "Big Query Create disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition"
          ),
        opt[String]("write_disposition")
          .action((x, c) => c.copy(writeDisposition = WriteDisposition.valueOf(x)))
          .text(
            "Big Query Write disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition"
          )
      )
    }
    OParser.parse(parser, args, JdbcLoadConfig())
  }
}
