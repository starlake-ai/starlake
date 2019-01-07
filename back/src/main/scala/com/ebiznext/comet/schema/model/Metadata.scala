package com.ebiznext.comet.schema.model

import com.ebiznext.comet.schema.model.Format.DSV
import com.ebiznext.comet.schema.model.Mode.FILE
import com.ebiznext.comet.schema.model.Write.APPEND
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}

@JsonDeserialize(using = classOf[MetadataDeserializer])
case class Metadata(
                     mode: Option[Mode] = None,
                     format: Option[Format] = None,
                     withHeader: Option[Boolean] = None,
                     separator: Option[String] = None,
                     quote: Option[String] = None,
                     escape: Option[String] = None,
                     write: Option[Write] = None,
                     partition: Option[List[String]] = None,
                     dateFormat: Option[String] = None,
                     timestampFormat: Option[String] = None
                   ) {
  override def toString: String =
    s"""
       |mode:${getMode()}
       |format:${getFormat()}
       |withHeader:${isWithHeader()}
       |separator:${getSeparator()}
       |quote:${getQuote()}
       |escape:${getEscape()}
       |write:${getWrite()}
       |partition:${getPartition()}
       |dateFormat:${getDateFormat()}
       |timestampFormat:${getTimestampFormat()}
       """.stripMargin

  def getMode(): Mode = mode.getOrElse(FILE)

  def getFormat(): Format = format.getOrElse(DSV)

  def isWithHeader(): Boolean = withHeader.getOrElse(true)

  def getSeparator(): String = separator.getOrElse(";")

  def getQuote(): String = quote.getOrElse("\"")

  def getEscape(): String = escape.getOrElse("\\")

  def getWrite(): Write = write.getOrElse(APPEND)

  def getPartition(): List[String] = partition.getOrElse(Nil)

  def getDateFormat() = dateFormat.getOrElse("yyyy-MM-dd")

  def getTimestampFormat() = timestampFormat.getOrElse("yyyy-MM-dd HH:mm:ss")

  def `import`(child: Metadata): Metadata = {
    def defined[T](parent: Option[T], child: Option[T]): Option[T] =
      if (child.isDefined) child else parent

    Metadata(
      defined(this.mode, child.mode),
      defined(this.format, child.format),
      defined(this.withHeader, child.withHeader),
      defined(this.separator, child.separator),
      defined(this.quote, child.quote),
      defined(this.escape, child.escape),
      defined(this.write, child.write),
      defined(this.partition, child.partition),
      defined(this.dateFormat, child.dateFormat),
      defined(this.timestampFormat, child.timestampFormat)
    )
  }
}


object Metadata {
  val CometPartitionColumns = List("comet_year", "comet_month", "comet_day", "comet_hour", "comet_minute")

  def Dsv(
           separator: Option[String],
           quote: Option[String],
           escape: Option[String],
           write: Option[Write]
         ) = new Metadata(
    Some(Mode.FILE),
    Some(Format.DSV),
    Some(true),
    separator,
    quote,
    escape,
    write,
    None,
    Some("yyyy-MM-dd"),
    Some("yyyy-MM-dd HH:mm:ss")
  )
}


class MetadataDeserializer extends JsonDeserializer[Metadata] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): Metadata = {
    val node: JsonNode = jp.getCodec().readTree(jp)

    def isNull(field: String): Boolean = node.get(field) == null || node.get(field).isNull

    val mode = if (isNull("mode")) None else Some(Mode.fromString(node.get("mode").asText))
    val format = if (isNull("format")) None else Some(Format.fromString(node.get("format").asText))
    val withHeader = if (isNull("withHeader")) None else Some(node.get("withHeader").asBoolean())
    val separator = if (isNull("separator")) None else Some(node.get("separator").asText)
    val quote = if (isNull("quote")) None else Some(node.get("quote").asText)
    val escape = if (isNull("escape")) None else Some(node.get("escape").asText)
    val write = if (isNull("write")) None else Some(Write.fromString(node.get("write").asText))
    import scala.collection.JavaConverters._
    val partition = if (isNull("partition")) None else Some(node.get("partition").asInstanceOf[ArrayNode].elements.asScala.toList.map(_.asText()))
    val dateFormat = if (isNull("dateFormat")) None else Some(node.get("dateFormat").asText)
    val timestampFormat = if (isNull("timestampFormat")) None else Some(node.get("timestampFormat").asText)
    Metadata(mode, format, withHeader, separator, quote, escape, write, partition, dateFormat, timestampFormat)
  }
}