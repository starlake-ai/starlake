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

import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{Domain, Schema, Type}
import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Encoders}

import scala.util.Try

/** Parse a simple one level json file. Complex types such as arrays & maps are not supported. Use
  * JsonIngestionJob instead. This class is for simple json only that makes it way faster.
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
  */
class SimpleJsonIngestionJob(
  domain: Domain,
  schema: Schema,
  types: List[Type],
  path: List[Path],
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  options: Map[String, String]
)(implicit settings: Settings)
    extends DsvIngestionJob(domain, schema, types, path, storageHandler, schemaHandler, options) {

  override protected def loadDataSet(): Try[DataFrame] = {
    Try {

      val dfIn =
        if (metadata.isArray()) {
          val jsonRDD =
            session.sparkContext.wholeTextFiles(path.map(_.toString).mkString(",")).map {
              case (_, content) => content
            }

          session.read
            .options(metadata.getOptions())
            .json(session.createDataset(jsonRDD)(Encoders.STRING))
            .withColumn(
              //  Spark cannot detect the input file automatically, so we should add it explicitly
              CometColumns.cometInputFileNameColumn,
              if (settings.comet.grouped) lit(path.map(_.toString).mkString(","))
              else lit(path.head.toString)
            )

        } else {
          session.read
            .option("encoding", metadata.getEncoding())
            .option("multiline", metadata.getMultiline())
            .options(metadata.getOptions())
            .json(path.map(_.toString): _*)
            .withColumn(
              //  Spark here can detect the input file automatically, so we're just using the input_file_name spark function
              CometColumns.cometInputFileNameColumn,
              org.apache.spark.sql.functions.input_file_name()
            )
        }

      logger.whenDebugEnabled {
        logger.debug(dfIn.schemaString())
      }

      val df = applyIgnore(dfIn)

      import session.implicits._
      if (df.columns.contains("_corrupt_record")) {
        // TODO send rejected records to rejected area
        logger.whenDebugEnabled {
          logger.debug(df.filter($"_corrupt_record".isNotNull).showString(1000, truncate = 0))
        }
        throw new Exception(
          s"""Invalid JSON File: ${path
              .map(_.toString)
              .mkString(",")}. SIMPLE_JSON require a valid json file """
        )
      } else {
        df
      }
    }
  }
}
