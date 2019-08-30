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

package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Encoders}

import scala.util.{Failure, Success, Try}

/**
  * Parse a simple one level json file. Complex types such as arrays & maps are not supported.
  * Use JsonIngestionJob instead.
  * This class is for simple json only that makes it way faster.
  *
  * @param domain         : Input Dataset Domain
  * @param schema         : Input Dataset Schema
  * @param types          : List of globally defined types
  * @param path           : Input dataset path
  * @param storageHandler : Storage Handler
  */
class SimpleJsonIngestionJob(
  domain: Domain,
  schema: Schema,
  types: List[Type],
  path: List[Path],
  storageHandler: StorageHandler
) extends DsvIngestionJob(domain, schema, types, path, storageHandler) {

  override def loadDataSet(): Try[DataFrame] = {
    try {

      val df =
        if (metadata.isArray()) {
          val jsonRDD =
            session.sparkContext.wholeTextFiles(path.map(_.toString).mkString(",")).map(x => x._2)
          session.read.json(session.createDataset(jsonRDD)(Encoders.STRING))
        } else {
          session.read
            .option("encoding", metadata.getEncoding())
            .option("multiline", metadata.getMultiline())
            .json(path.map(_.toString): _*)
        }
      df.printSchema()
      import session.implicits._
      val resDF = if (df.columns.contains("_corrupt_record")) {
        //TODO send rejected records to rejected area
        df.filter($"_corrupt_record".isNotNull).show(100, false)
        throw new Exception(
          s"""Invalid JSON File: ${path
            .map(_.toString)
            .mkString(",")}. SIMPLE_JSON require a valid json file """
        )
      } else {
        df
      }
      Success(resDF)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }
}
