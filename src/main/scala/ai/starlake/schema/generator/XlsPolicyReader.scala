package ai.starlake.schema.generator

import ai.starlake.schema.model._
import org.apache.poi.ss.usermodel._

import java.io.File

class XlsPolicyReader(input: Input) extends XlsModel {

  private val workbook: Workbook = input match {
    case InputPath(s)  => WorkbookFactory.create(new File(s))
    case InputFile(in) => WorkbookFactory.create(in)
  }

  lazy val policies: List[RowLevelSecurity] = {
    val sheet = Option(workbook.getSheet("_policies")).getOrElse(workbook.getSheet("policies"))
    val (rows, headerMap) = getColsOrder(sheet, allPolicyHeaders.map { case (k, _) => k })
    rows.flatMap { row =>
      val nameOpt =
        Option(row.getCell(headerMap("_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val predicateOpt =
        Option(row.getCell(headerMap("_predicate"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val grantsOpt =
        Option(row.getCell(headerMap("_grants"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val descriptionOpt =
        Option(row.getCell(headerMap("_description"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      (nameOpt, predicateOpt, grantsOpt, descriptionOpt) match {
        case (Some(name), predicate, Some(grants), description) =>
          Some(
            RowLevelSecurity(
              name = name,
              predicate = predicate.getOrElse("TRUE"),
              grants = grants.replaceAll("\\s*,\\s*", ",").replaceAll("\\s+", ",").split(',').toSet,
              description = description.getOrElse("")
            )
          )
        case _ => None
      }
    }.toList
  }
}
