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

package com.ebiznext.comet.schema.model

import java.util.regex.Pattern

import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable

/**
  * How dataset are merge
  *
  * @param key    list of attributes to join existing with incoming data. Use renamed columns here.
  * @param delete Optional valid sql condition on the incoming dataset. Use renamed column here.
  */
case class MergeOptions(key: List[String], delete: Option[String] = None)

/**
  * Dataset Schema
  *
  * @param name       : Schema name, must be unique in the domain. Will become the hive table name
  * @param pattern    : filename pattern to which this schema must be applied
  * @param attributes : datasets columns
  * @param metadata   : Dataset metadata
  * @param comment    : free text
  * @param presql     :  SQL code executed before the file is ingested
  * @param postsql    : SQL code executed right after the file has been ingested
  */
case class Schema(
  name: String,
  pattern: Pattern,
  attributes: List[Attribute],
  metadata: Option[Metadata],
  merge: Option[MergeOptions],
  comment: Option[String],
  presql: Option[List[String]],
  postsql: Option[List[String]]
) {

  /**
    * @return Are the parittions columns defined in the metadata valid column names
    */
  def validatePartitionColumns(): Boolean = {
    metadata.forall(
      _.getPartitionAttributes().forall(
        attributes
          .map(_.getFinalName())
          .union(Metadata.CometPartitionColumns)
          .contains
      )
    )
  }

  /**
    * This Schema as a Spark Catalyst Schema
    *
    * @param types : globally defined types
    * @return Spark Catalyst Schema
    */
  def sparkType(): StructType = {
    val fields = attributes.map { attr =>
      StructField(attr.name, attr.sparkType(), !attr.required)
    }
    StructType(fields)
  }

  /**
    * return the list of renamed attributes
    *
    * @return list of tuples (oldname, newname)
    */
  def renamedAttributes(): List[(String, String)] = {
    attributes.filter(attr => attr.name != attr.getFinalName()).map { attr =>
      (attr.name, attr.getFinalName())
    }
  }

  /**
    * Check attribute definition correctness :
    *   - schema name should be a valid table identifier
    *   - attribute name should be a valid Hive column identifier
    *   - attribute name can occur only once in the schema
    *
    * @param types : List of globally defined types
    * @return error list or true
    */
  def checkValidity(types: List[Type]): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val tableNamePattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]{1,256}")
    if (!tableNamePattern.matcher(name).matches())
      errorList += s"Schema with name $name should respect the pattern ${tableNamePattern.pattern()}"

    attributes.foreach { attribute =>
      for (errors <- attribute.checkValidity(types).left) {
        errorList ++= errors
      }
    }

    val duplicateErrorMessage =
      "%s is defined %d times. An attribute can only be defined once."
    for (errors <- duplicates(attributes.map(_.name), duplicateErrorMessage).left) {
      errorList ++= errors
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def discreteAttrs(): List[Attribute] = attributes.filter(_.getMetricType() == MetricType.DISCRETE)

  def continuousAttrs(): List[Attribute] =
    attributes.filter(_.getMetricType() == MetricType.CONTINUOUS)

  def mappingConfig(): EsMapping =
    this.metadata.flatMap(_.mapping).getOrElse(EsMapping(Some(s"$name/$name"), None, None))

  def mapping(template: Option[String]): String = {
    val attrs = attributes.map(_.mapping()).mkString(",")
    val properties =
      s"""
         |"properties": {
         |$attrs
         |}
      """.stripMargin

    template.getOrElse {
      s"""
         |{
         |  "index_patterns": ["$name", "$name-*"],
         |  "settings": {
         |    "number_of_shards": "1",
         |    "number_of_replicas": "0"
         |  },
         |  "mappings": {
         |    "_doc": {
         |      "_source": {
         |        "enabled": true
         |      },
         |      __PROPERTIES__
         |    }
         |  }
         |}
      """.stripMargin.replace("__PROPERTIES__", properties)
    }

  }
}
