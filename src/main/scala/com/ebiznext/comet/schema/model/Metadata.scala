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

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.SchemaHandler
import com.ebiznext.comet.schema.model.Format.DSV
import com.ebiznext.comet.schema.model.Mode.FILE
import com.ebiznext.comet.schema.model.WriteMode.APPEND
import com.fasterxml.jackson.annotation.JsonIgnore

import scala.collection.mutable

/**
  * Specify Schema properties.
  * These properties may be specified at the schema or domain level
  * Any property non specified at the schema level is taken from the
  * one specified at the domain level or else the default value is returned.
  *
  * @param mode       : FILE mode by default
  * @param format     : DSV by default
  * @param encoding   : UTF-8 by default
  * @param multiline  : are json objects on a single line or multiple line ? Single by default.  false means single. false also means faster
  * @param array      : Is a json stored as a single object array ? false by default
  * @param withHeader : does the dataset has a header ? true bu default
  * @param separator  : the column separator,  ';' by default
  * @param quote      : The String quote char, '"' by default
  * @param escape     : escaping char '\' by default
  * @param write      : Write mode, APPEND by default
  * @param partition  : Partition columns, no partitioning by default
  * @param sink       : should the dataset be indexed in elasticsearch after ingestion ?
  * @param ignore     : Pattern to ignore org UDF to apply to ignore some lines
  */
case class Metadata(
  mode: Option[Mode] = None,
  format: Option[Format] = None,
  encoding: Option[String] = None,
  multiline: Option[Boolean] = None,
  array: Option[Boolean] = None,
  withHeader: Option[Boolean] = None,
  separator: Option[String] = None,
  quote: Option[String] = None,
  escape: Option[String] = None,
  write: Option[WriteMode] = None,
  partition: Option[Partition] = None,
  sink: Option[Sink] = None,
  ignore: Option[String] = None
) {

  override def toString: String =
    s"""
       |mode:${getMode()}
       |format:${getFormat()}
       |encoding:${getEncoding()}
       |multiline:${getMultiline()}
       |array:${isArray()}
       |withHeader:${isWithHeader()}
       |separator:${getSeparator()}
       |quote:${getQuote()}
       |escape:${getEscape()}
       |write:${getWrite()}
       |partition:${getPartitionAttributes()}
       |sink:${getSink()}
       """.stripMargin

  def getMode(): Mode = mode.getOrElse(FILE)

  def getFormat(): Format = format.getOrElse(DSV)

  def getEncoding(): String = encoding.getOrElse("UTF-8")

  def getMultiline(): Boolean = multiline.getOrElse(false)

  def isArray(): Boolean = array.getOrElse(false)

  def isWithHeader(): Boolean = withHeader.getOrElse(true)

  def getSeparator(): String = separator.getOrElse(";")

  def getQuote(): String = quote.getOrElse("\"")

  def getEscape(): String = escape.getOrElse("\\")

  def getWrite(): WriteMode = write.getOrElse(APPEND)

  @JsonIgnore
  def getPartitionAttributes(): List[String] = partition.map(_.getAttributes()).getOrElse(Nil)

  @JsonIgnore
  def getSamplingStrategy(): Double = partition.map(_.getSampling()).getOrElse(0.0)

  def getSink(): Option[Sink] = sink

  /**
    * Merge a single attribute
    *
    * @param parent : Domain level metadata attribute
    * @param child  : Schema level metadata attribute
    * @return attribute if merge, the domain attribute otherwise.
    */
  protected def merge[T](parent: Option[T], child: Option[T]): Option[T] =
    if (child.isDefined) child else parent

  /**
    * Merge this metadata with its child.
    * Any property defined at the child level overrides the one defined at this level
    * This allow a schema to override the domain metadata attribute
    * Applied to a Domain level metadata
    *
    * @param child : Schema level metadata
    * @return the metadata resulting of the merge of the schema and the domain metadata.
    */
  def `import`(child: Metadata): Metadata = {
    Metadata(
      mode = merge(this.mode, child.mode),
      format = merge(this.format, child.format),
      encoding = merge(this.encoding, child.encoding),
      multiline = merge(this.multiline, child.multiline),
      array = merge(this.array, child.array),
      withHeader = merge(this.withHeader, child.withHeader),
      separator = merge(this.separator, child.separator),
      quote = merge(this.quote, child.quote),
      escape = merge(this.escape, child.escape),
      write = merge(this.write, child.write),
      partition = merge(this.partition, child.partition),
      sink = merge(this.sink, child.sink),
      ignore = merge(this.ignore, child.ignore)
    )
  }

  def checkValidity(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Either[List[String], Boolean] = {
    def isIgnoreUDF = ignore.map(_.startsWith("udf:")).getOrElse(false)
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty

    if (!isIgnoreUDF && getFormat() == Format.DSV)
      errorList += "When input format is DSV, ignore metadata attribute cannot be a regex, it must be an UDF"

    if (
      ignore.isDefined && !List(Format.DSV, Format.SIMPLE_JSON, Format.POSITION).contains(
        getFormat()
      )
    )
      errorList += s"ignore not yet supported for format ${getFormat()}"

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }
}

object Metadata {

  /**
    * Predefined partition columns.
    */
  val CometPartitionColumns =
    List("comet_year", "comet_month", "comet_day", "comet_hour", "comet_minute")
}
