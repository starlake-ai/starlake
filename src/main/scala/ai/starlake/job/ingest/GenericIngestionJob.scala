/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.job.ingest

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import java.sql._
import java.time.LocalDateTime
import java.util.Properties
import scala.util.Try

/** Main class to ingest delimiter separated values file
  *
  * @param domain
  *   : Input Dataset Domain
  * @param schema
  *   : Input Dataset Schema
  * @param types
  *   : List of globally defined types
  * @param path
  *   : Input dataset path
  * @param storageHandler
  *   : Storage Handler
  * @param options
  *   : Parameters to pass as input (k1=v1,k2=v2,k3=v3)
  */
class GenericIngestionJob(
  val domain: Domain,
  val schema: Schema,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler,
  val options: Map[String, String]
)(implicit val settings: Settings)
    extends IngestionJob {

  /** @return
    *   Spark Job name
    */
  override def name: String =
    s"""${domain.name}-${schema.name}-${path.headOption.map(_.getName).mkString(",")}"""

  /** dataset Header names as defined by the schema
    */
  val schemaHeaders: List[String] = schema.attributes.map(_.name)

  private def getConnection(
    url: String,
    user: Option[String],
    password: Option[String]
  ): Try[Connection] =
    Try {
      val info = new Properties
      user.foreach(user => info.put("user", user))
      password.foreach(password => info.put("password", password))
      DriverManager.getConnection(url, info)
    }

  private def executeQuery[T](conn: Connection, query: String, apply: ResultSet => T): Try[T] =
    Try {
      val stmt = conn.createStatement
      val rs = stmt.executeQuery(query)
      val result = apply(rs)
      rs.close()
      stmt.close()
      result
    }

  private def executeUpdate(conn: Connection, stmt: PreparedStatement): Try[Int] =
    Try {
      val count = stmt.executeUpdate()
      stmt.close()
      count
    }

  trait SQlRequest[T] {
    def getResult(resultSet: ResultSet): T
  }

  class LastExportDateRequest(domainName: String, schemaName: String)
      extends SQlRequest[java.sql.Timestamp] {
    val queryString =
      s"SELECT max(timestamp) FROM STARLAKE_DELTA where domain like '$domainName' and schema like '$schemaName'"
    def getResult(resultSet: ResultSet): java.sql.Timestamp = resultSet.getTimestamp(0)
  }

  class NewExportDateRequest(
    dbtable: String,
    timestampColumn: String,
    domainName: String,
    schemaName: String,
    lastExportDate: Timestamp
  ) extends SQlRequest[java.sql.Timestamp] {
    val queryString =
      s"SELECT max($timestampColumn) FROM $dbtable WHERE $timestampColumn > '$lastExportDate'"
    def getResult(resultSet: ResultSet): java.sql.Timestamp = resultSet.getTimestamp(0)
  }

  class CountRowsRequest(
    dbtable: String,
    timestampColumn: String,
    domainName: String,
    schemaName: String,
    lastExportDate: Timestamp,
    newExportDate: Timestamp
  ) extends SQlRequest[Int] {
    val queryString =
      s"SELECT COUNT(*) FROM $dbtable WHERE $timestampColumn > '$lastExportDate' AND $timestampColumn <= '$newExportDate'"
    def getResult(resultSet: ResultSet): Int = resultSet.getInt(0)
  }

  case class DeltaRow(
    domain: String,
    schema: String,
    timestamp: Timestamp,
    duration: Int,
    mode: String,
    count: Long,
    success: Boolean,
    message: String,
    step: String
  )
  def updateStatement(
    conn: Connection,
    row: DeltaRow
  ): Try[PreparedStatement] = {
    Try {
      val sqlInsert =
        s"insert into STARLAKE_DELTA(domain, schema, timestamp, duration, mode, count, success, message, step) values(?, ?, ?, ?, ?, ?, ?, ?, ?)"
      val preparedStatement = conn.prepareStatement(sqlInsert)
      preparedStatement.setString(1, row.domain)
      preparedStatement.setString(2, row.schema)
      preparedStatement.setTimestamp(3, row.timestamp)
      preparedStatement.setInt(4, row.duration)
      preparedStatement.setString(5, row.mode)
      preparedStatement.setLong(6, row.count)
      preparedStatement.setBoolean(7, row.success)
      preparedStatement.setString(8, row.message)
      preparedStatement.setString(9, row.step)
      preparedStatement
    }
  }

  /** Load dataset using spark csv reader and all metadata. Does not infer schema. columns not
    * defined in the schema are dropped fro the dataset (require datsets with a header)
    *
    * @return
    *   Spark Dataset
    */
  protected def loadDataSet(): Try[DataFrame] = {
    Try {
      val options = metadata.getOptions()
      val timestampColumn = options.get("_timestamp")
      val startTime = Timestamp.valueOf(LocalDateTime.now())
      for {
        url      <- Try(options("url"))
        user     <- Try(options.get("user"))
        password <- Try(options.get("password"))
        conn     <- getConnection(url, user, password)
        startLoadStatement <- updateStatement(
          conn,
          DeltaRow(
            domain.name,
            schema.name,
            startTime,
            -1,
            timestampColumn.map(_ => "DELTA").getOrElse("FULL"),
            -1L,
            true,
            "Starting ...",
            "1.START_LOAD"
          )
        )
        _ <- executeUpdate(conn, startLoadStatement)
      } {

        timestampColumn.map { timestampColumn =>
          val dbtable = options.getOrElse(
            "dbtable",
            throw new Exception(
              s"${domain.name}.${schema.name}: dbtable should be present when timestamp attribute is set "
            )
          )
          for {
            lastExportDateRequest <- Try(new LastExportDateRequest(domain.name, schema.name))
            lastExportDate <- executeQuery(
              conn,
              lastExportDateRequest.queryString,
              lastExportDateRequest.getResult
            )
            newExportDateRequest <- Try(
              new NewExportDateRequest(
                dbtable,
                timestampColumn,
                domain.name,
                schema.name,
                lastExportDate
              )
            )
            newExportDate <- executeQuery(
              conn,
              newExportDateRequest.queryString,
              newExportDateRequest.getResult
            )

            countRowsRequest <- Try(
              new CountRowsRequest(
                dbtable,
                timestampColumn,
                domain.name,
                schema.name,
                lastExportDate,
                newExportDate
              )
            )
            countRows <- executeQuery(
              conn,
              countRowsRequest.queryString,
              countRowsRequest.getResult
            )
          } yield null
        }
      }
      val format = metadata.getOptions().getOrElse("format", "jdbc")
      val dfIn = session.read
        .options(options - "_timestamp")
        .format(format)
        .load()

      logger.debug(dfIn.schema.treeString)
      if (dfIn.limit(1).count() == 0) {
        // empty dataframe with accepted schema
        val sparkSchema = schema.sparkSchemaWithoutScriptedFields(schemaHandler)

        session
          .createDataFrame(session.sparkContext.emptyRDD[Row], StructType(sparkSchema))
          .withColumn(
            CometColumns.cometInputFileNameColumn,
            org.apache.spark.sql.functions.input_file_name()
          )
      } else {
        val df = applyIgnore(dfIn)

        val datasetHeaders = df.columns.toList
        val (_, drop) = intersectHeaders(datasetHeaders, schemaHeaders)
        if (datasetHeaders.length == drop.length) {
          throw new Exception(s"""No attribute found in input dataset ${path.toString}
                                 | SchemaHeaders : ${schemaHeaders.mkString(",")}
                                 | Dataset Headers : ${datasetHeaders.mkString(",")}
             """.stripMargin)
        }
        val resDF = df.drop(drop: _*)
        resDF.withColumn(
          //  Spark here can detect the input file automatically, so we're just using the input_file_name spark function
          CometColumns.cometInputFileNameColumn,
          org.apache.spark.sql.functions.input_file_name()
        )
      }
    }
  }

  /** Apply the schema to the dataset. This is where all the magic happen Valid records are stored
    * in the accepted path / table and invalid records in the rejected path / table
    *
    * @param dataset
    *   : Spark Dataset
    */
  protected def ingest(dataset: DataFrame): (Dataset[String], Dataset[Row]) = {
    val orderedAttributes = reorderAttributes(dataset)
    val (orderedTypes, orderedSparkTypes) = reorderTypes(orderedAttributes)
    val validationResult = flatRowValidator.validate(
      session,
      metadata.getFormat(),
      metadata.getSeparator(),
      dataset,
      orderedAttributes,
      orderedTypes,
      orderedSparkTypes,
      settings.comet.privacy.options,
      settings.comet.cacheStorageLevel,
      settings.comet.sinkReplayToFile
    )

    saveRejected(validationResult.errors, validationResult.rejected)
    saveAccepted(validationResult)
    (validationResult.errors, validationResult.accepted)
  }
}

/*

domain: DOMAIN
schema: SCHEMA
metadata:
  options:
      _timestamp: updated_at
      dbtable: SCHEMA.TABLE
      url: ...


Create the following table:
CREATE TABLE (DOMAIN, SCHEMA, LAST_EXPORT_DATE, LAST_EXPORT_TYPE, NB_ROWS_LAST_EXPORT)


last_export_date =  SELECT max(timestamp_column) FROM delta
new_export_date = SELECT MAX(timestamp_column) FROM dbtable where timestamp_column > last_export_date
count = SELECT count(*) FROM dbtable where timestamp_column > last_export_date
df = SELECT * FROM dbtable where timestamp_column > last_export_date
insert into delta (domain, schema, new_export_date, DELTA, count)

 */
