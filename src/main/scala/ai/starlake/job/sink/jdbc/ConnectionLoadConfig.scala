package ai.starlake.job.sink.jdbc

import ai.starlake.config.Settings
import ai.starlake.schema.model.RowLevelSecurity
import ai.starlake.utils.CliConfig
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import org.apache.spark.sql.DataFrame
import scopt.OParser

import java.sql.{DriverManager, SQLException}
import scala.util.{Failure, Success, Try}

case class ConnectionLoadConfig(
  sourceFile: Either[String, DataFrame] = Left(""),
  outputTable: String = "",
  createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED,
  writeDisposition: WriteDisposition = WriteDisposition.WRITE_APPEND,
  format: String = "jdbc",
  mode: Option[String] = None,
  options: Map[String, String] = Map.empty,
  rls: Option[List[RowLevelSecurity]] = None
)

object ConnectionLoadConfig extends CliConfig[ConnectionLoadConfig] {

  def checkTablePresent(
    jdbcOptions: Settings.Connection,
    jdbcEngine: Settings.JdbcEngine,
    outputTable: String
  ): Unit = {
    assert(jdbcOptions.format == "jdbc")
    val table = jdbcEngine.tables.get(outputTable)
    table.foreach { table =>
      val conn = DriverManager.getConnection(
        jdbcOptions.options("url"),
        jdbcOptions.options("user"),
        jdbcOptions.options("password")
      )

      Try {
        val stmt = conn.createStatement
        Try {
          val pingSql = table.effectivePingSql(outputTable)
          val rs = stmt.executeQuery(pingSql)
          rs.close() // we don't need to fetch the result, it should be empty anyway.
        } match {
          case Failure(e) if e.isInstanceOf[SQLException] =>
            stmt.executeUpdate(table.createSql)
            conn.commit() // some databases are transactional wrt schema updates
          case Success(_) => ;
        }
        stmt.close()
      }
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
    options: Map[String, String],
    createTableIfAbsent: Boolean = true
  ): ConnectionLoadConfig = {
    // TODO: wanted to just call this "apply" but I'd need to get rid of the defaults in the ctor above

    val jdbcOptions = comet.connections(jdbcName)
    val isJDBC = jdbcOptions.format == "jdbc"

    if (createTableIfAbsent && isJDBC) {
      comet.jdbcEngines.get(jdbcOptions.engine).foreach { jdbcEngine =>
        checkTablePresent(jdbcOptions, jdbcEngine, outputTable)
      }
    }

    ConnectionLoadConfig(
      sourceFile = sourceFile,
      outputTable = outputTable,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      jdbcOptions.format,
      jdbcOptions.mode,
      jdbcOptions.options ++ options
    )
  }

  val parser: OParser[Unit, ConnectionLoadConfig] = {
    val builder = OParser.builder[ConnectionLoadConfig]
    import builder._
    OParser.sequence(
      programName("starlake cnxload"),
      head("starlake", "cnxload", "[options]"),
      note("""
          |Load parquet file into JDBC Table.
          |""".stripMargin),
      opt[String]("source_file")
        .action((x, c) => c.copy(sourceFile = Left(x)))
        .text("Full Path to source file")
        .required(),
      opt[String]("output_table")
        .action((x, c) => c.copy(outputTable = x))
        .text("JDBC Output Table")
        .required(),
      opt[Map[String, String]]("options")
        .action((x, c) => c.copy(options = x))
        .text(
          "Connection options eq for jdbc : driver, user, password, url, partitions, batchSize"
        ),
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
  def parse(args: Seq[String]): Option[ConnectionLoadConfig] =
    OParser.parse(parser, args, ConnectionLoadConfig())
}
