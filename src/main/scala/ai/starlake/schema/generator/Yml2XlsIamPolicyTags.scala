package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.poi.xssf.usermodel.XSSFWorkbook

object Yml2XlsIamPolicyTags extends LazyLogging with XlsModel {
  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    Yml2XlsIamPolicyTagsCmd.run(args, new SchemaHandler(settings.storageHandler()))
  }

  def writeXls(iamPolicyTags: IamPolicyTags, folder: String): Unit = {
    val xlsOut = File(folder, "iam-policy-tags.xlsx")
    val workbook = new XSSFWorkbook()
    val font = workbook.createFont
    font.setFontHeightInPoints(14.toShort)
    font.setFontName("Calibri")
    font.setBold(true)
    val sheet = workbook.createSheet("IAM Policy Tags")
    fillHeaders(workbook, allIamPolicyTagHeaders, sheet)
    iamPolicyTags.iamPolicyTags.zipWithIndex.foreach { case (iamPolicyTag, rowIndex) =>
      val policyRow = sheet.createRow(2 + rowIndex)
      policyRow.createCell(0).setCellValue(iamPolicyTag.policyTag)
      policyRow.createCell(1).setCellValue(iamPolicyTag.members.mkString(","))
      policyRow.createCell(2).setCellValue(iamPolicyTag.role)
    }
    for (i <- allIamPolicyTagHeaders.indices)
      sheet.autoSizeColumn(i)
    xlsOut.delete(swallowIOExceptions = true)
    workbook.write(xlsOut.newFileOutputStream(false))
  }
}
