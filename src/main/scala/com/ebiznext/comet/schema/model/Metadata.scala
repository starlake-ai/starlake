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

import com.ebiznext.comet.schema.model.Format.DSV
import com.ebiznext.comet.schema.model.Mode.FILE
import com.ebiznext.comet.schema.model.WriteMode.APPEND
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}

import scala.language.postfixOps

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
  * @param index      : should the dataset be indexed in elasticsearch after ingestion ?
  */
@JsonDeserialize(using = classOf[MetadataDeserializer])
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
  index: Option[IndexSink] = None,
  properties: Option[Map[String, String]] = None,
  xml: Option[Map[String, String]] = None
) {

  override def toString: String =
    s"""
       |mode:${getIngestMode()}
       |format:${getFormat()}
       |encoding:${getEncoding()}
       |multiline:${getMultiline()}
       |array:${isArray()}
       |withHeader:${isWithHeader()}
       |separator:${getSeparator()}
       |quote:${getQuote()}
       |escape:${getEscape()}
       |write:${getWriteMode()}
       |partition:${getPartitionAttributes()}
       |index:${getIndexSink()}
       |properties:${properties}
       |xml:${xml}
       """.stripMargin

  def getIngestMode(): Mode = mode.getOrElse(FILE)

  def getFormat(): Format = format.getOrElse(DSV)

  def getEncoding(): String = encoding.getOrElse("UTF-8")

  def getMultiline(): Boolean = multiline.getOrElse(false)

  def isArray(): Boolean = array.getOrElse(false)

  def isWithHeader(): Boolean = withHeader.getOrElse(true)

  def getSeparator(): String = separator.getOrElse(";")

  def getQuote(): String = quote.getOrElse("\"")

  def getEscape(): String = escape.getOrElse("\\")

  def getWriteMode(): WriteMode = write.getOrElse(APPEND)

  def getPartitionAttributes(): List[String] = partition.map(_.getAtrributes()).getOrElse(Nil)

  def getSamplingStrategy(): Double = partition.map(_.getSampling()).getOrElse(0.0)

  def getIndexSink(): Option[IndexSink] = index

  def getProperties(): Map[String, String] = properties.getOrElse(Map.empty)

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
      index = merge(this.index, child.index),
      properties = merge(this.properties, child.properties),
      xml = merge(this.xml, child.xml)
    )
  }
}

object Metadata {

  /**
    * Predefined partition columns.
    */
  val CometPartitionColumns =
    List("comet_year", "comet_month", "comet_day", "comet_hour", "comet_minute")

  def Dsv(
    separator: Option[String],
    quote: Option[String],
    escape: Option[String],
    write: Option[WriteMode]
  ) = new Metadata(
    Some(Mode.FILE),
    Some(Format.DSV),
    None,
    Some(false),
    Some(false),
    Some(true),
    separator,
    quote,
    escape,
    write,
    None,
    None
  )
}

class MetadataDeserializer extends JsonDeserializer[Metadata] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Metadata = {
    val node: JsonNode = jp.getCodec().readTree[JsonNode](jp)

    def isNull(node: JsonNode, field: String): Boolean =
      node.get(field) == null || node.get(field).isNull

    val mode =
      if (isNull(node, "mode")) None
      else Some(Mode.fromString(node.get("mode").asText))
    val format =
      if (isNull(node, "format")) None
      else Some(Format.fromString(node.get("format").asText))
    val encoding =
      if (isNull(node, "encoding")) None
      else Some(node.get("encoding").asText)
    val multiline =
      if (isNull(node, "multiline")) None else Some(node.get("multiline").asBoolean())
    val array =
      if (isNull(node, "array")) None else Some(node.get("array").asBoolean())
    val withHeader =
      if (isNull(node, "withHeader")) None
      else Some(node.get("withHeader").asBoolean())
    val separator =
      if (isNull(node, "separator")) None else Some(node.get("separator").asText)
    val quote = if (isNull(node, "quote")) None else Some(node.get("quote").asText)
    val escape = if (isNull(node, "escape")) None else Some(node.get("escape").asText)
    val write =
      if (isNull(node, "write")) None
      else Some(WriteMode.fromString(node.get("write").asText))
    val partition =
      if (isNull(node, "partition")) None
      else
        Some(
          new PartitionDeserializer().deserialize(node.get("partition"))
        )
    val index =
      if (isNull(node, "index")) None else Some(IndexSink.fromString(node.get("index").asText))
    val mapping: Option[Map[String, String]] =
      if (isNull(node, "properties"))
        None
      else {
        val mappingField = node.get("properties")
        import scala.collection.JavaConverters._
        val fields = mappingField
          .fieldNames()
          .asScala
          .map { fieldName => (fieldName, mappingField.get(fieldName).asText()) } toMap

        Some(fields)
      }
    val xml: Option[Map[String, String]] =
      if (isNull(node, "xml"))
        None
      else {
        val xmlField = node.get("xml")
        import scala.collection.JavaConverters._
        val fields = xmlField
          .fieldNames()
          .asScala
          .map { fieldName => (fieldName, xmlField.get(fieldName).asText()) } toMap

        Some(fields)
      }
    Metadata(
      mode,
      format,
      encoding,
      multiline,
      array,
      withHeader,
      separator,
      quote,
      escape,
      write,
      partition,
      index,
      mapping,
      xml
    )
  }
}
