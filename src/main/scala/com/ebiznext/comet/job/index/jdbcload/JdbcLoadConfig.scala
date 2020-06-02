package com.ebiznext.comet.job.index.jdbcload

import java.sql.{DriverManager, SQLException}

import buildinfo.BuildInfo
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.CliConfig
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

object JdbcLoadConfig extends CliConfig[JdbcLoadConfig] {

  def checkTablePresent(
    jdbcName: String,
    comet: Settings.Comet,
    jdbcOptions: Settings.Jdbc,
    jdbcEngine: Settings.JdbcEngine,
    outputTable: String
  ): Unit = {
    val table = jdbcEngine.tables(outputTable)

    val conn = DriverManager.getConnection(jdbcOptions.uri, jdbcOptions.user, jdbcOptions.password)

    try {
      val stmt = conn.createStatement
      try {
        val pingSql = table.effectivePingSql
        val rs = stmt.executeQuery(pingSql)
        rs.close() // we don't need to fetch the result, it should be empty anyway.
      } catch {
        case _: SQLException =>
          stmt.executeUpdate(table.createSql)
          conn.commit() // some databases are transactional wrt schema updates 🥰
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }

  }

  def fromComet(
    jdbcName: String,
    comet: Settings.Comet,
    sourceFile: Either[String, DataFrame],
    outputTable: String,
    createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED,
    writeDisposition: WriteDisposition = WriteDisposition.WRITE_APPEND,
    partitions: Int = 1,
    batchSize: Int = 1000,
    createTableIfAbsent: Boolean = true
  ): JdbcLoadConfig = {
    // TODO: wanted to just call this "apply" but I'd need to get rid of the defaults in the ctor above

    val jdbcOptions = comet.jdbc(jdbcName)
    val jdbcEngine = comet.jdbcEngines(jdbcOptions.engine)
    val outputTableName = jdbcEngine.tables(outputTable).name

    if (createTableIfAbsent) {
      checkTablePresent(jdbcName, comet, jdbcOptions, jdbcEngine, outputTable)
    }

    JdbcLoadConfig(
      sourceFile = sourceFile,
      outputTable = outputTableName,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      jdbcEngine.driver,
      jdbcOptions.uri,
      jdbcOptions.user,
      jdbcOptions.password
    )
  }

  val parser: OParser[Unit, JdbcLoadConfig] = {
    val builder = OParser.builder[JdbcLoadConfig]
    import builder._
    OParser.sequence(
      programName("comet"),
      head("comet", BuildInfo.version),
      opt[String]("source_file")
        .action((x, c) => c.copy(sourceFile = Left(x)))
        .text("Full Path to source file")
        .required(),
      opt[String]("output_table")
        .action((x, c) => c.copy(outputTable = x))
        .text("JDBC Output Table")
        .required(),
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

  // comet bqload  --source_file xxx --output_table schema --source_format parquet --create_disposition  CREATE_IF_NEEDED --write_disposition WRITE_TRUNCATE
  //               --partitions 1  --batch_size 1000 --user username --password pwd -- url jdbcurl
  def parse(args: Seq[String]): Option[JdbcLoadConfig] =
    OParser.parse(parser, args, JdbcLoadConfig())
}
