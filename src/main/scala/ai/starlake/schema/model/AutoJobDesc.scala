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

package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler

import scala.collection.mutable
import scala.util.Try

/** A job is a set of transform tasks executed using the specified engine.
  *
  * @param name:
  *   Job logical name
  * @param tasks
  *   List of transform tasks to execute
  * @param format
  *   output file format when using Spark engine. Ingored for BigQuery. Default value is "parquet"
  * @param coalesce
  *   When outputting files, should we coalesce it to a single file. Useful when CSV is the output
  *   format.
  * @param udf
  *   : Register UDFs written in this JVM class when using Spark engine Register UDFs stored at this
  *   location when using BigQuery engine
  * @param views
  *   : Create temporary views using where the key is the view name and the map the SQL request
  *   corresponding to this view using the SQL engine supported syntax.
  * @param engine
  *   : SPARK or BQ. Default value is SPARK.
  */
case class AutoJobDesc(
  name: String,
  tasks: List[AutoTaskDesc],
  taskRefs: List[String] = Nil,
  format: Option[String] = None,
  coalesce: Option[Boolean] = None,
  udf: Option[String] = None,
  schedule: Map[String, String] = Map.empty,
  sink: Option[AllSinks] = None,
  tags: Set[String] = Set.empty
) extends Named {
  def this() = this("", Nil) // Should never be called. Here for Jackson deserialization only
  // TODO
  def checkValidity(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Either[(List[String], List[String]), Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val warningList: mutable.MutableList[String] = mutable.MutableList.empty

    // Check Domain name validity
    val forceJobPrefixRegex = settings.comet.forceJobPattern.r
    // TODO: name doesn't need to respect the pattern because it may be renamed. Restriction is based on target database restriction.
    // We may check depending on the sink type but we may sink differently for each table.
    // It would be better to assume a starlake pattern to describe a dataset and the container of the dataset such as the bigquery syntax project:dataset
    // and then apply this syntax to all databases even if natively they don't accept that.
    // Therefore, it means that we need to adapt on writing to the database, the target name.
    // The same applies to table name.
    if (!forceJobPrefixRegex.pattern.matcher(name).matches())
      errorList += s"name: Job with name $name should respect the pattern ${forceJobPrefixRegex.regex}"
    if (errorList.nonEmpty)
      Left(errorList.toList, warningList.toList)
    else
      Right(true)
  }

  def getEngine(implicit settings: Settings): Engine = {
    val connectionRef =
      sink.flatMap { sink => sink.connectionRef }.getOrElse(settings.comet.connectionRef)
    val connection = settings.comet
      .connection(connectionRef)
      .getOrElse(throw new Exception("Connection not found"))
    connection.getEngine()
  }

  def aclTasks(): List[AutoTaskDesc] = tasks.filter { task =>
    task.acl.nonEmpty
  }

  def rlsTasks(): List[AutoTaskDesc] = tasks.filter { task =>
    task.rls.nonEmpty
  }
}

/** A field in the schema. For struct fields, the field "attributes" contains all sub attributes
  *
  * @param name
  *   : Attribute name as defined in the source dataset and as received in the file
  * @param comment
  *   : free text for attribute description
  */
case class AttributeDesc(
  name: String,
  comment: String = "",
  accessPolicy: Option[String] = None
) {
  def this() = this("") // Should never be called. Here for Jackson deserialization only
}

object AutoJobDesc {
  private def diffTasks(
    existing: List[AutoTaskDesc],
    incoming: List[AutoTaskDesc]
  ): (List[AutoTaskDesc], List[AutoTaskDesc], List[AutoTaskDesc]) = {
    val (commonTasks, deletedTasks) =
      existing
        // .filter(_.name.nonEmpty)
        .partition(task => incoming.map(_.name.toLowerCase).contains(task.name.toLowerCase))
    val addedTasks =
      incoming.filter(task =>
        task.name.nonEmpty && !existing.map(_.name.toLowerCase).contains(task.name.toLowerCase)
      )
    (addedTasks, deletedTasks, commonTasks)
  }

  def compare(existing: AutoJobDesc, incoming: AutoJobDesc): Try[JobDiff] = {
    Try {
      val (addedTasks, deletedTasks, existingCommonTasks) =
        diffTasks(existing.tasks, incoming.tasks)

      val commonTasks: List[(AutoTaskDesc, AutoTaskDesc)] = existingCommonTasks.map { task =>
        (
          task,
          incoming.tasks
            .find(_.name.toLowerCase() == task.name.toLowerCase())
            .getOrElse(throw new Exception("Should not happen"))
        )
      }

      val updatedTasksDiff = commonTasks.map { case (existing, incoming) =>
        AutoTaskDesc.compare(existing, incoming)
      }
      val taskRefsDiff =
        AnyRefDiff.diffSetString("taskRefs", existing.taskRefs.toSet, incoming.taskRefs.toSet)
      val formatDiff = AnyRefDiff.diffOptionString("format", existing.format, incoming.format)
      val coalesceDiff = AnyRefDiff.diffOptionString(
        "coalesce",
        existing.coalesce.map(_.toString),
        incoming.coalesce.map(_.toString)
      )
      val udfDiff = AnyRefDiff.diffOptionString("udf", existing.udf, incoming.udf)
      val scheduleDiff = AnyRefDiff.diffMap("schedule", existing.schedule, incoming.schedule)
      JobDiff(
        existing.name,
        TasksDiff(addedTasks.map(_.name), deletedTasks.map(_.name), updatedTasksDiff),
        taskRefsDiff,
        formatDiff,
        coalesceDiff,
        udfDiff,
        scheduleDiff
      )
    }
  }
}
