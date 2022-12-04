package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.model.WriteMode.OVERWRITE
import ai.starlake.schema.model.{Domain, PrivacyLevel, Schema}

/** Params for the script's mustache template
  * @param domainToExport
  *   Domain name
  * @param tableToExport
  *   table to export
  * @param columnsToExport
  *   cols to export
  * @param fullExport
  *   if table is going to be fully or delta exported
  * @param deltaColumn
  *   if delta exported, which is the col holding the date of last update
  * @param dsvDelimiter
  *   export result dsv delimiter
  * @param exportOutputFileBase
  *   export dsv file base name (will be completed by current datetime when executed)
  * @param scriptOutputFile
  *   where the script is produced
  * @param auditDB
  *   Schema name where the audit table is created. Same as domainToExport by default
  */
case class TemplateParams(
  domainToExport: String,
  tableToExport: String,
  columnsToExport: List[(String, String, Boolean, PrivacyLevel)],
  fullExport: Boolean,
  deltaColumn: Option[String],
  dsvDelimiter: String,
  auditDB: Option[String],
  activeEnv: Map[String, String]
) {

  val paramMap: Map[String, Any] = {

    // This is how we deal with the last element not needing a trailing a comma in a Mustache template
    val columnsParam: List[Map[String, Any]] = columnsToExport match {
      case (name, tpe, ignore, privacyLevel) :: Nil =>
        List(
          Map(
            "name"              -> name.toLowerCase(),
            "type"              -> tpe,
            "trailing_col_char" -> "",
            "ignore"            -> ignore,
            "privacyLevel"      -> privacyLevel.toString
          )
        )
      case Nil => Nil
      case atLeastTwoElemList =>
        val allButLast = atLeastTwoElemList.dropRight(1)
        val (lastName, lastType, lastIgnore, lastPrivacyLevel) = atLeastTwoElemList.last
        allButLast
          .map { case (name, tpe, ignore, privacyLevel) =>
            Map(
              "name"              -> name.toLowerCase(),
              "type"              -> tpe,
              "ignore"            -> ignore,
              "privacyLevel"      -> privacyLevel.toString,
              "trailing_col_char" -> ","
            )
          } :+ Map(
          "name"              -> lastName.toLowerCase(),
          "type"              -> lastType,
          "ignore"            -> lastIgnore,
          "privacyLevel"      -> lastPrivacyLevel,
          "trailing_col_char" -> ""
        )
    }
    deltaColumn
      .foldLeft(
        List(
          "domain"       -> domainToExport,
          "table"        -> tableToExport.toLowerCase,
          "domain_table" -> domainToExport, // For compatibility
          "table_name"   -> tableToExport.toLowerCase, // For compatibility
          "delimiter"    -> dsvDelimiter,
          "columns"      -> columnsParam,
          "full_export"  -> fullExport,
          "audit_schema" -> auditDB.getOrElse(domainToExport)
        )
      ) { case (list, deltaCol) => list :+ ("delta_column" -> deltaCol.toUpperCase) }
      .toMap ++ activeEnv
  }
}

object TemplateParams {

  /** Generating all the TemplateParams, corresponding to all the schema's tables of the domain
    *
    * @param domain
    *   The domain
    * @param scriptsOutputFolder
    *   Where the scripts are produced
    * @param defaultDeltaColumn
    *   The default delta column to use
    * @param deltaColumns
    *   A table name -> delta column to use mapping (if needing a special delta column for a given
    *   table). Has precedence over `defaultDeltaColumn`.
    * @return
    */
  def fromDomain(
    domain: Domain,
    defaultDeltaColumn: Option[String],
    deltaColumns: Map[String, String],
    activeEnv: Map[String, String]
  )(implicit settings: Settings): List[TemplateParams] =
    domain.tables.map(table =>
      fromSchema(
        domain.name,
        table,
        deltaColumns.get(table.name).orElse(defaultDeltaColumn),
        activeEnv
      )
    )

  /** Generate scripts template parameters, extracting the tables and the columns described in the
    * schema
    * @param schema
    *   The schema used to generate the scripts parameters
    * @param scriptsOutputFolder
    *   Where the scripts are produced
    * @param deltaColumn
    *   The delta column to use for that table
    * @return
    *   The corresponding TemplateParams
    */
  def fromSchema(
    domainName: String,
    schema: Schema,
    deltaColumn: Option[String],
    activeEnv: Map[String, String]
  )(implicit settings: Settings): TemplateParams = {
    // exportFileBase is the csv file name base such as EXPORT_L58MA_CLIENT_DELTA_...
    // Considering a pattern like EXPORT_L58MA_CLIENT.*.csv
    // The script which is generated will append the current date time to that base (EXPORT_L58MA_CLIENT_18032020173100).
    val exportFileBase = s"${schema.pattern.toString.split("\\.\\*").head}"
    val isFullExport = schema.metadata.flatMap(_.write).contains(OVERWRITE)
    new TemplateParams(
      domainToExport = domainName,
      tableToExport = schema.name,
      columnsToExport = schema.attributes
        .filter(_.script.isEmpty)
        .map(col => (col.name, col.`type`, col.isIgnore(), col.getPrivacy())),
      fullExport = isFullExport,
      deltaColumn = if (!isFullExport) deltaColumn else None,
      dsvDelimiter = schema.metadata.flatMap(_.separator).getOrElse(","),
      None,
      activeEnv
    )
  }
}
