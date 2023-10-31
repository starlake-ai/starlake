package ai.starlake.job.sink.jdbc

import ai.starlake.config.Settings
import ai.starlake.schema.model.{ConnectionType, RowLevelSecurity}
import ai.starlake.utils.CliConfig
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import org.apache.spark.sql.DataFrame
import scopt.OParser

import java.sql.{DriverManager, SQLException}
import scala.util.{Failure, Success, Try}
import ai.starlake.utils.Formatter.RichFormatter

case class JdbcConnectionLoadConfig(
  sourceFile: Either[String, DataFrame] = Left(""),
  outputTable: String = "",
  createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED,
  writeDisposition: WriteDisposition = WriteDisposition.WRITE_APPEND,
  format: String = "jdbc",
  options: Map[String, String] = Map.empty,
  rls: Option[List[RowLevelSecurity]] = None
)

object JdbcConnectionLoadConfig extends CliConfig[JdbcConnectionLoadConfig] {
  val command = "cnxload"
  def checkTablePresent(
    jdbcOptions: Settings.Connection,
    jdbcEngine: Settings.JdbcEngine,
    outputTable: String
  )(implicit settings: Settings): Unit = {
    assert(
      jdbcOptions.getType() == ConnectionType.JDBC,
      s"Only JDBC connections are supported ${jdbcOptions.getType()}"
    )
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
            stmt.executeUpdate(table.createSql.richFormat(Map("table" -> outputTable), Map.empty))
            conn.commit() // some databases are transactional wrt schema updates
          case Success(_) => ;
        }
        stmt.close()
      }
      conn.close()
    }
  }

  def fromComet(
    connectionRef: String,
    comet: Settings.AppConfig,
    sourceFile: Either[String, DataFrame],
    outputTable: String,
    createDisposition: CreateDisposition, // = CreateDisposition.CREATE_IF_NEEDED,
    writeDisposition: WriteDisposition, // = WriteDisposition.WRITE_APPEND,
    createTableIfAbsent: Boolean = true
  )(implicit settings: Settings): JdbcConnectionLoadConfig = {
    // TODO: wanted to just call this "apply" but I'd need to get rid of the defaults in the ctor above

    val jdbcOptions = comet.connections(connectionRef)
    val jdbcEngineName = jdbcOptions.getJdbcEngineName()
    if (createTableIfAbsent)
      comet.jdbcEngines.get(jdbcEngineName.toString).foreach { jdbcEngine =>
        checkTablePresent(jdbcOptions, jdbcEngine, outputTable)
      }

    // This is to make sure that the column names are uppercase on JDBC databases
    // TODO: Once spark 3.3 is not supported anymore, switch to withColumnsRenamed(colsMap: Map[String, String])
    val dfWithUppercaseColumns = sourceFile.map { df =>
      df.columns.foldLeft(df) { case (df, colName) =>
        df.withColumnRenamed(colName, colName.toUpperCase())
      }
    }

    JdbcConnectionLoadConfig(
      sourceFile = dfWithUppercaseColumns,
      outputTable = outputTable.toUpperCase(),
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      jdbcOptions.sparkFormat.getOrElse("jdbc"),
      jdbcOptions.options
    )
  }

  val parser: OParser[Unit, JdbcConnectionLoadConfig] = {
    val builder = OParser.builder[JdbcConnectionLoadConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
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
  def parse(args: Seq[String]): Option[JdbcConnectionLoadConfig] =
    OParser.parse(parser, args, JdbcConnectionLoadConfig())
}
