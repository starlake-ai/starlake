package ai.starlake.schema.generator

import ai.starlake.schema.model._
import org.apache.poi.ss.usermodel._

import java.io.File

class XlsPolicyReader(input: Input) extends XlsModel {

  private val workbook: Workbook = input match {
    case InputPath(s)  => WorkbookFactory.create(new File(s))
    case InputFile(in) => WorkbookFactory.create(in)
  }

  lazy val policies: List[SecurityLevel] = {
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
      val aclOpt =
        Option(row.getCell(headerMap("_acl"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .forall(_.toBoolean)
      val rlsOpt =
        Option(row.getCell(headerMap("_rls"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .forall(_.toBoolean)
      (nameOpt, predicateOpt, grantsOpt, descriptionOpt, aclOpt, rlsOpt) match {
        case (Some(name), predicate, Some(grants), description, aclOpt, rlsOpt) =>
          val _grants = grants
            .replaceAll("\\s*,\\s*", ",")
            .replaceAll("\\s+", ",")
            .split(',')
            .toSet
          val _predicate = predicate.getOrElse("TRUE")
          val acl = AccessControlEntry("roles/bigquery.dataViewer", _grants, name)
          val rls = RowLevelSecurity(
            name = name,
            predicate = _predicate,
            grants = _grants,
            description = description.getOrElse("")
          )
          if (aclOpt && rlsOpt) {
            List(
              rls,
              acl
            )
          } else if (aclOpt) {
            Some(acl)
          } else if (rlsOpt) {
            Some(rls)
          } else if (_predicate.toUpperCase() == "TRUE") { // for backward compatibility
            Some(acl)
          } else {
            Some(rls)
          }
        case _ => None
      }
    }.toList
  }
}
