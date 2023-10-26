package ai.starlake.schema.generator

import org.apache.poi.ss.usermodel.{Cell, DataFormatter, Row, Sheet, Workbook}
import org.apache.poi.xssf.usermodel.XSSFSheet

import scala.jdk.CollectionConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}

trait XlsModel {

  val allDomainHeaders = List(
    "_name"        -> "Name",
    "_path"        -> "Directory",
    "_ack"         -> "Ack",
    "_description" -> "Description",
    "_rename"      -> "Rename"
  )
  val allPolicyHeaders = List(
    "_name"        -> "Name",
    "_predicate"   -> "Predicate",
    "_grants"      -> "User Groups",
    "_description" -> "Description"
  )

  val allIamPolicyTagHeaders = List(
    "_policyTag" -> "Policy Tag",
    "_members"   -> "Members",
    "_role"      -> "Role"
  )

  val allSchemaHeaders = List(
    "_name"               -> "Name",
    "_pattern"            -> "Pattern",
    "_mode"               -> "FILE or STREAM",
    "_write"              -> "Write Mode\n(OVERWRITE, APPEND, ERROR_IF_EXISTS)",
    "_format"             -> "DSV, POSITION, XML, JSON",
    "_header"             -> "Hash header (true / false)",
    "_delimiter"          -> "Separator",
    "_delta_column"       -> "Timestamp column to use on merge",
    "_merge_keys"         -> "Merge columns",
    "_description"        -> "Description",
    "_encoding"           -> "File encoding (UTF-8 by default)",
    "_sampling"           -> "Sampling strategy(obsolete)",
    "_partitioning"       -> "partition columns",
    "_sink"               -> "Sink Type",
    "_clustering"         -> "Clustering columns",
    "_merge_query_filter" -> "Filter to use on merge",
    "_presql"             -> "Pre SQLs - ###",
    "_postsql"            -> "Post SQLs - ###",
    "_primary_key"        -> "Primary Key",
    "_tags"               -> "Tags",
    "_rename"             -> "Rename",
    "_long_name"          -> "Rename source table",
    "_policy"             -> "Access Policy",
    "_escape"             -> "Escaping Char",
    // new fields
    "_multiline"  -> "Multiline",
    "_array"      -> "Is JSON array",
    "_quote"      -> "Quote character",
    "_ignore"     -> "UDF to apply to ignore input lines",
    "_xml"        -> "XML Options",
    "_extensions" -> "Accepted extensions",
    "_options"    -> "Spark ingestion options",
    "_validator"  -> "Class validator"
  )

  val allAttributeHeaders = List(
    "_name"           -> "Name",
    "_rename"         -> "New Name",
    "_type"           -> "Semantic Type",
    "_required"       -> "Required(true / false)",
    "_privacy"        -> "Privacy (MD5, SHA1, Initials ...)",
    "_metric"         -> "Metric (CONTINUOUS, DISCRETE ...)",
    "_default"        -> "Default value",
    "_script"         -> "Script",
    "_description"    -> "Description",
    "_position_start" -> "Start Position",
    "_position_end"   -> "End Position",
    "_trim"           -> "Trim (LEFT, RIGHT,BOTH)",
    "_ignore"         -> "Ignore ?",
    "_foreign_key"    -> "Foreign Key",
    "_tags"           -> "Tags",
    "_policy"         -> "Access Policy"
  )

  val allSchemaJobHeaders = List(
    "_job"         -> "Job Name",
    "_domain"      -> "Domain",
    "_name"        -> "Name",
    "_source"      -> "Tables sources for job",
    "_write"       -> "Write Mode\n(OVERWRITE, APPEND, ERROR_IF_EXISTS)",
    "_frequency"   -> "Job frenquency",
    "_partition"   -> "Partition column",
    "_description" -> "Description",
    "_policy"      -> "Access Policy",
    "_database"    -> "Database"
  )

  val allAttributeJobHeaders = List(
    "_name"        -> "Name",
    "_type"        -> "Semantic Type",
    "_required"    -> "Required(true / false)",
    "_script"      -> "Script",
    "_description" -> "Description"
  )

  protected def getColsOrder(
    sheet: Sheet,
    allHeaders: List[String]
  ): (Iterable[Row], Map[String, Int]) = {
    val scalaSheet = sheet.asScala
    if (scalaSheet == null) {
      throw new IllegalArgumentException("Sheet is empty")
    }
    val hasSchema = scalaSheet.head
      .getCell(0, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      .getStringCellValue
      .startsWith("_")
    if (hasSchema) {
      val headersRow = scalaSheet.head
      val headerMap = headersRow
        .cellIterator()
        .asScala
        .zipWithIndex
        .map { case (headerCell, index) =>
          val header = headerCell.getStringCellValue
          (header, index)
        }
        .toMap
      (scalaSheet.drop(2), headerMap)
    } else {
      (scalaSheet.drop(1), allHeaders.zipWithIndex.toMap)
    }
  }

  object formatter {
    private val f = new DataFormatter()

    def formatCellValue(cell: Cell): Option[String] = {
      // remove all no-breaking spaces from cell to avoid parsing errors
      f.formatCellValue(cell).trim.replaceAll("\\u00A0", "") match {
        case v if v.isEmpty => None
        case v              => Some(v)
      }
    }
  }

  def fillHeaders(workbook: Workbook, headers: List[(String, String)], sheet: XSSFSheet): Unit = {
    val font = workbook.createFont
    font.setFontHeightInPoints(14.toShort)
    font.setFontName("Calibri")
    font.setBold(true)
    val boldStyle = workbook.createCellStyle()
    boldStyle.setFont(font)
    val header = sheet.createRow(0)
    header.setHeight(0) // Hide header
    headers.map { case (key, _) => key }.zipWithIndex.foreach { case (key, columnIndex) =>
      val cell = header.createCell(columnIndex)
      cell.setCellValue(key)
    }
    val labelHeader = sheet.createRow(1)
    headers.map { case (_, value) => value }.zipWithIndex.foreach { case (value, columnIndex) =>
      val cell = labelHeader.createCell(columnIndex)
      cell.setCellValue(value)
      cell.setCellStyle(boldStyle)
    }
  }

}
