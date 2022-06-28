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

import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Format.DSV
import ai.starlake.schema.model.Mode.FILE
import ai.starlake.schema.model.WriteMode.APPEND
import com.fasterxml.jackson.annotation.JsonIgnore

import scala.collection.mutable

/** Specify Schema properties. These properties may be specified at the schema or domain level Any
  * property not specified at the schema level is taken from the one specified at the domain level
  * or else the default value is returned.
  *
  * @param mode
  *   : FILE mode by default. FILE and STREAM are the two accepted values. FILE is currently the
  *   only supported mode.
  * @param format
  *   : DSV by default. Supported file formats are :
  *   - DSV : Delimiter-separated values file. Delimiter value iss specified in the "separator"
  *     field.
  *   - POSITION : FIXED format file where values are located at an exact position in each line.
  *   - SIMPLE_JSON : For optimisation purpose, we differentiate JSON with top level values from
  *     JSON with deep level fields. SIMPLE_JSON are JSON files with top level fields only.
  *   - JSON : Deep JSON file. Use only when your json documents contain subdocuments, otherwise
  *     prefer to use SIMPLE_JSON since it is much faster.
  *   - XML : XML files
  * @param encoding
  *   : UTF-8 if not specified.
  * @param multiline
  *   : are json objects on a single line or multiple line ? Single by default. false means single.
  *   false also means faster
  * @param array
  *   : Is the json stored as a single object array ? false by default. This means that by default
  *   we have on json document per line.
  * @param withHeader
  *   : does the dataset has a header ? true bu default
  * @param separator
  *   : the values delimiter, ';' by default value may be a multichar string starting from Spark3
  * @param quote
  *   : The String quote char, '"' by default
  * @param escape
  *   : escaping char '\' by default
  * @param write
  *   : Write mode, APPEND by default
  * @param partition
  *   : Partition columns, no partitioning by default
  * @param sink
  *   : should the dataset be indexed in elasticsearch after ingestion ?
  * @param ignore
  *   : Pattern to ignore or UDF to apply to ignore some lines
  * @param clustering
  *   : List of attributes to use for clustering
  * @param xml
  *   : com.databricks.spark.xml options to use (eq. rowTag)
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
  ignore: Option[String] = None,
  clustering: Option[Seq[String]] = None,
  xml: Option[Map[String, String]] = None,
  directory: Option[String] = None,
  extensions: Option[List[String]] = None,
  ack: Option[String] = None,
  options: Option[Map[String, String]] = None,
  validator: Option[String] = None,
  schedule: Option[Map[String, String]] = None
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
       |xml:${xml}
       |directory:${directory}
       |extensions:${extensions}
       |ack:${ack}
       |options:${options}
       |validator:${validator}
       |schedule:${schedule}
       |""".stripMargin

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

  @JsonIgnore
  def getOptions(): Map[String, String] = options.getOrElse(Map.empty)

  @JsonIgnore
  def getXmlOptions(): Map[String, String] = xml.getOrElse(Map.empty)

  @JsonIgnore
  def getXsdPath(): Option[String] = {
    val xmlOptions = xml.getOrElse(Map.empty)
    xmlOptions.get("rowValidationXSDPath").orElse(xmlOptions.get("xsdPath"))
  }

  /** Merge a single attribute
    *
    * @param parent
    *   : Domain level metadata attribute
    * @param child
    *   : Schema level metadata attribute
    * @return
    *   attribute if merge, the domain attribute otherwise.
    */
  protected def merge[T](parent: Option[T], child: Option[T]): Option[T] =
    if (child.isDefined) child else parent

  /** Merge this metadata with its child. Any property defined at the child level overrides the one
    * defined at this level This allow a schema to override the domain metadata attribute Applied to
    * a Domain level metadata
    *
    * @param child
    *   : Schema level metadata
    * @return
    *   the metadata resulting of the merge of the schema and the domain metadata.
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
      ignore = merge(this.ignore, child.ignore),
      xml = merge(this.xml, child.xml),
      directory = merge(this.directory, child.directory),
      extensions = merge(this.extensions, child.extensions),
      ack = merge(this.ack, child.ack),
      options = merge(this.options, child.options),
      validator = merge(this.validator, child.validator),
      schedule = merge(this.schedule, child.schedule)
    )
  }

  def checkValidity(
    schemaHandler: SchemaHandler
  ): Either[List[String], Boolean] = {
    def isIgnoreUDF = ignore.forall(_.startsWith("udf:"))
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

  /** Predefined partition columns.
    */
  val CometPartitionColumns =
    List("comet_date", "comet_year", "comet_month", "comet_day", "comet_hour", "comet_minute")
}
