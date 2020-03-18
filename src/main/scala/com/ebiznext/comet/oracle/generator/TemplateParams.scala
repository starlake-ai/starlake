package com.ebiznext.comet.oracle.generator

import java.time.format.DateTimeFormatter

import better.files.File
import com.ebiznext.comet.schema.model.{Domain, Schema, WriteMode}

/**
  * Params for the script's mustache template
  * @param tableToExport table to export
  * @param columnsToExport cols to export
  * @param isDelta if table is going to be fully or delta exported
  * @param deltaColumn if delta exported, which is the col holding the date of last update
  * @param dsvDelimiter export result dsv delimiter
  * @param exportOutputFileBase export dsv file base name (will be completed by current datetime when executed)
  * @param scriptOutputFile where the script is produced
  */
case class TemplateParams(
  tableToExport: String,
  columnsToExport: List[String],
  isDelta: Boolean,
  deltaColumn: Option[String],
  dsvDelimiter: String,
  exportOutputFileBase: String,
  scriptOutputFile: File
) {

  def toParamMap: Map[String, Any] = Map(
    "table_name"       -> tableToExport.toUpperCase,
    "delimiter"        -> dsvDelimiter,
    "columns"          -> columnsToExport.map(_.toUpperCase).mkString(", "),
    "output_file_base" -> exportOutputFileBase,
    "delta_column"     -> deltaColumn.map(_.toUpperCase).getOrElse(""),
    "is_delta"         -> isDelta
  )
}

object TemplateParams {

  val dateFormater = DateTimeFormatter.ISO_DATE

  /**
    * Generating all the TemplateParams, corresponding to all the schema's tables of the domain
    * @param domain The domain
    * @param scriptsOutputFolder where the scripts are produced
    * @return
    */
  def fromDomain(
    domain: Domain,
    scriptsOutputFolder: File
  ): List[TemplateParams] =
    domain.schemas.map(fromSchema(_, scriptsOutputFolder))

  /**
    * Generate N Oracle sqlplus scripts template parameters, extracting the tables and the columns described in the schema
    * @param schema The schema used to generate the scripts parameters
    * @return The corresponding TemplateParams
    */
  def fromSchema(
    schema: Schema,
    scriptsOutputFolder: File
  ): TemplateParams = {
    val exportType: String = schema.metadata.flatMap(_.write).fold("FULL") {
      case WriteMode.APPEND => "DELTA"
      case _                => "FULL"
    }
    val scriptOutputFileName = s"EXTRACT_${schema.name}_$exportType.sql"
    // exportFileBase is the csv file name base such as EXPORT_L58MA_CLIENT_DELTA_...
    // Considering a pattern like EXPORT_L58MA_CLIENT_*
    // The script which is generated will append the current date time to that base (EXPORT_L58MA_CLIENT_18032020173100).
    val exportFileBase = s"${schema.pattern.toString.split("\\_\\*").head}_$exportType"
    new TemplateParams(
      tableToExport = schema.name,
      columnsToExport = schema.attributes.map(_.name),
      isDelta = schema.metadata.flatMap(_.write).contains(WriteMode.APPEND),
      deltaColumn = schema.merge.flatMap(_.timestamp),
      dsvDelimiter = schema.metadata.flatMap(_.separator).getOrElse(","),
      exportOutputFileBase = exportFileBase,
      scriptOutputFile = scriptsOutputFolder / scriptOutputFileName
    )
  }
}
