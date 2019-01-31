package com.ebiznext.comet.schema.model

import com.ebiznext.comet.schema.model.Format.DSV
import com.ebiznext.comet.schema.model.Mode.FILE
import com.ebiznext.comet.schema.model.WriteMode.APPEND
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}

/**
  * Specify Schema properties.
  * These properties may be specified at the schema or domain level
  * Any property non specified at the schema level is taken from the
  * one specified at the domain level or else the default value is returned.
  *
  * @param mode            : FILE mode by default
  * @param format          : DSV by default
  * @param multiline       : are json objects on a single line or multiple line ? Single by default.  false means single. false also means faster
  * @param array           : Is a json stored as a single object array ? false by default
  * @param withHeader      : does the dataset has a header ? true bu default
  * @param separator       : the column separator,  ';' by default
  * @param quote           : The String quote char, '"' by default
  * @param escape          : escaping char '\' by default
  * @param write           : Write mode, APPEND by default
  * @param partition       : Partition columns, no partitioning by default
  * @param dateFormat      : Column date format when primitive type is date, yyyy-MM-dd  by default
  * @param timestampFormat : Column timestamp format when primitive type is timestamp, "yyyy-MM-dd HH:mm:ss by default
  */
@JsonDeserialize(using = classOf[MetadataDeserializer])
case class Metadata(
                     mode: Option[Mode] = None,
                     format: Option[Format] = None,
                     multiline: Option[Boolean] = None,
                     array: Option[Boolean] = None,
                     withHeader: Option[Boolean] = None,
                     separator: Option[String] = None,
                     quote: Option[String] = None,
                     escape: Option[String] = None,
                     write: Option[WriteMode] = None,
                     partition: Option[List[String]] = None
                   ) {
  override def toString: String =
    s"""
       |mode:${getIngestMode()}
       |format:${getFormat()}
       |multiline:${getMultiline()}
       |array:${isArray()}
       |withHeader:${isWithHeader()}
       |separator:${getSeparator()}
       |quote:${getQuote()}
       |escape:${getEscape()}
       |write:${getWriteMode()}
       |partition:${getPartition()}
       """.stripMargin

  def getIngestMode(): Mode = mode.getOrElse(FILE)

  def getFormat(): Format = format.getOrElse(DSV)

  def getMultiline(): Boolean = multiline.getOrElse(false)

  def isArray(): Boolean = array.getOrElse(false)

  def isWithHeader(): Boolean = withHeader.getOrElse(true)

  def getSeparator(): String = separator.getOrElse(";")

  def getQuote(): String = quote.getOrElse("\"")

  def getEscape(): String = escape.getOrElse("\\")

  def getWriteMode(): WriteMode = write.getOrElse(APPEND)

  def getPartition(): List[String] = partition.getOrElse(Nil)

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
    /**
      * Merge a single attribute
      *
      * @param parent : Domain level metadata attribute
      * @param child  : Schema level metadata attribute
      * @return attribute if defined, the domain attribute otherwise.
      */
    def defined[T](parent: Option[T], child: Option[T]): Option[T] =
      if (child.isDefined) child else parent

    Metadata(
      mode = defined(this.mode, child.mode),
      format = defined(this.format, child.format),
      multiline = defined(this.multiline, child.multiline),
      array = defined(this.array, child.array),
      withHeader = defined(this.withHeader, child.withHeader),
      separator = defined(this.separator, child.separator),
      quote = defined(this.quote, child.quote),
      escape = defined(this.escape, child.escape),
      write = defined(this.write, child.write),
      partition = defined(this.partition, child.partition)
    )
  }
}


object Metadata {
  /**
    * Predefined partition columns.
    */
  val CometPartitionColumns = List("comet_year", "comet_month", "comet_day", "comet_hour", "comet_minute")

  def Dsv(
           separator: Option[String],
           quote: Option[String],
           escape: Option[String],
           write: Option[WriteMode]
         ) = new Metadata(
    Some(Mode.FILE),
    Some(Format.DSV),
    Some(false),
    Some(false),
    Some(true),
    separator,
    quote,
    escape,
    write,
    None
  )
}


class MetadataDeserializer extends JsonDeserializer[Metadata] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Metadata = {
    val node: JsonNode = jp.getCodec().readTree(jp)

    def isNull(field: String): Boolean = node.get(field) == null || node.get(field).isNull

    val mode = if (isNull("mode")) None else Some(Mode.fromString(node.get("mode").asText))
    val format = if (isNull("format")) None else Some(Format.fromString(node.get("format").asText))
    val multiline = if (isNull("multiline")) None else Some(node.get("multiline").asBoolean())
    val array = if (isNull("array")) None else Some(node.get("array").asBoolean())
    val withHeader = if (isNull("withHeader")) None else Some(node.get("withHeader").asBoolean())
    val separator = if (isNull("separator")) None else Some(node.get("separator").asText)
    val quote = if (isNull("quote")) None else Some(node.get("quote").asText)
    val escape = if (isNull("escape")) None else Some(node.get("escape").asText)
    val write = if (isNull("write")) None else Some(WriteMode.fromString(node.get("write").asText))
    import scala.collection.JavaConverters._
    val partition = if (isNull("partition")) None else Some(node.get("partition").asInstanceOf[ArrayNode].elements.asScala.toList.map(_.asText()))
    Metadata(mode, format, multiline, array, withHeader, separator, quote, escape, write, partition)
  }
}