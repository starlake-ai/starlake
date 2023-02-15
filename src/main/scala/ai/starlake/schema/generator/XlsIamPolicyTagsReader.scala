package ai.starlake.schema.generator

import ai.starlake.schema.model._
import org.apache.poi.ss.usermodel._

import java.io.File

import scala.jdk.CollectionConverters._

class XlsIamPolicyTagsReader(input: Input) extends XlsModel {

  private val workbook: Workbook = input match {
    case InputPath(s)  => WorkbookFactory.create(new File(s))
    case InputFile(in) => WorkbookFactory.create(in)
  }

  lazy val iamPolicyTags: List[IamPolicyTag] = {
    val sheets = workbook.sheetIterator().asScala.toList
    val sheet = Option(
      workbook
        .getSheet("_iam_policy_tags")
    )
      .orElse(sheets.find(_.getSheetName.toLowerCase() == "iam policy tags"))
    sheet.map { sheet =>
      val (rows, headerMap) = getColsOrder(sheet, allIamPolicyTagHeaders.map { case (k, _) => k })
      rows.flatMap { row =>
        val policyTagOpt =
          Option(row.getCell(headerMap("_policyTag"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
            .flatMap(formatter.formatCellValue)
        val membersOpt =
          Option(row.getCell(headerMap("_members"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
            .flatMap(formatter.formatCellValue)
            .map(_.split(","))
        val roleOpt =
          Option(row.getCell(headerMap("_role"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
            .flatMap(formatter.formatCellValue)

        (policyTagOpt, membersOpt, roleOpt) match {
          case (Some(policyTag), Some(members), Some(role)) =>
            Some(
              IamPolicyTag(
                policyTag,
                members.toList,
                role
              )
            )
          case _ => None
        }
      }.toList
    }
  }.getOrElse(Nil)
}
