package com.ebiznext.comet.schema.generator

import better.files.File
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.SchemaHandler
import com.ebiznext.comet.schema.model.Domain
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.xssf.usermodel.XSSFSheet

class Yml2XlsWriter(schemaHandler: SchemaHandler) extends LazyLogging with XlsModel {

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    Yml2XlsConfig.parse(args) match {
      case Some(config) =>
        generateXls(config.domains, config.xlsDirectory)
      case _ =>
        println(Yml2XlsConfig.usage())
    }
  }

  def generateXls(domainNames: Seq[String], outputDir: String)(implicit
    settings: Settings
  ): Unit = {
    val domains =
      domainNames match {
        case Nil => schemaHandler.domains
        case x   => schemaHandler.domains.filter(domain => x.contains(domain.name))
      }
    domains.foreach { domain =>
      writeDomainXls(domain, outputDir)
    }
  }

  def writeDomainXls(domain: Domain, folder: String): Unit = {
    def fillHeaders(headers: List[(String, String)], sheet: XSSFSheet): Unit = {
      val header = sheet.createRow(0)
      headers.map(_._1).zipWithIndex.foreach { case (value, columnIndex) =>
        val cell = header.createCell(columnIndex)
        cell.setCellValue(value)
      }
      val labelHeader = sheet.createRow(1)
      headers.map(_._2).zipWithIndex.foreach { case (value, columnIndex) =>
        val cell = labelHeader.createCell(columnIndex)
        cell.setCellValue(value)
      }
    }
    val xlsOut = File(folder, domain.name + ".xlsx")
    import org.apache.poi.xssf.usermodel.XSSFWorkbook
    val workbook = new XSSFWorkbook()
    val domainSheet = workbook.createSheet("domain")
    fillHeaders(allDomainHeaders, domainSheet)
    val domainRow = domainSheet.createRow(2)
    domainRow.createCell(0).setCellValue(domain.name)
    domainRow.createCell(1).setCellValue(domain.directory)
    domainRow.createCell(2).setCellValue(domain.ack.getOrElse(""))
    domainRow.createCell(3).setCellValue(domain.comment.getOrElse(""))

    val schemaSheet = workbook.createSheet("schemas")
    fillHeaders(allSchemaHeaders, schemaSheet)
    domain.schemas.zipWithIndex.foreach { case (schema, rowIndex) =>
      val metadata = schema.mergedMetadata(domain.metadata)
      val schemaRow = schemaSheet.createRow(2 + rowIndex)
      schemaRow.createCell(0).setCellValue(schema.name)
      schemaRow.createCell(1).setCellValue(schema.pattern.toString)
      schemaRow.createCell(2).setCellValue(metadata.mode.map(_.toString).getOrElse(""))
      schemaRow.createCell(3).setCellValue(metadata.write.map(_.toString).getOrElse(""))
      schemaRow.createCell(4).setCellValue(metadata.format.map(_.toString).getOrElse(""))
      schemaRow.createCell(5).setCellValue(metadata.withHeader.map(_.toString).getOrElse(""))
      schemaRow.getCell(5).setCellType(CellType.BOOLEAN)
      schemaRow.createCell(6).setCellValue(metadata.getSeparator())

      schema.merge.foreach { mergeOptions =>
        schemaRow.createCell(7).setCellValue(mergeOptions.timestamp.getOrElse(""))
        schemaRow.createCell(8).setCellValue(mergeOptions.key.mkString(","))
        schemaRow.createCell(15).setCellValue(mergeOptions.queryFilter.getOrElse(""))
      }
      schemaRow.createCell(9).setCellValue(schema.comment.getOrElse(""))
      schemaRow.createCell(10).setCellValue(metadata.encoding.getOrElse(""))
      schemaRow
        .createCell(11)
        .setCellValue(metadata.partition.map(_.getSampling().toString).getOrElse(""))
      schemaRow
        .createCell(12)
        .setCellValue(metadata.partition.map(_.getAttributes().mkString(",")).getOrElse(""))
      schemaRow
        .createCell(13)
        .setCellValue(metadata.getSink().map(_.toString).getOrElse(""))
      schemaRow
        .createCell(14)
        .setCellValue(metadata.clustering.map(_.mkString(",")).getOrElse(""))

      val attributesSheet = workbook.createSheet(schema.name)
      fillHeaders(allAttributeHeaders, attributesSheet)
      schema.attributes.zipWithIndex.foreach { case (attr, rowIndex) =>
        val attrRow = attributesSheet.createRow(2 + rowIndex)
        attrRow.createCell(0).setCellValue(attr.name)
        attrRow.createCell(1).setCellValue(attr.rename.getOrElse(""))
        attrRow.createCell(2).setCellValue(attr.`type`)
        attrRow.createCell(3).setCellValue(attr.required)
        attrRow.getCell(3).setCellType(CellType.BOOLEAN)
        attrRow.createCell(4).setCellValue(attr.privacy.toString)
        attrRow.createCell(5).setCellValue(attr.metricType.map(_.toString).getOrElse(""))
        attrRow.createCell(6).setCellValue(attr.default.getOrElse(""))
        attrRow.createCell(7).setCellValue(attr.script.getOrElse(""))
        attrRow.createCell(8).setCellValue(attr.comment.getOrElse(""))
        attrRow.createCell(9).setCellValue(attr.position.map(_.first.toString).getOrElse(""))
        attrRow.getCell(9).setCellType(CellType.NUMERIC)
        attrRow.createCell(10).setCellValue(attr.position.map(_.last.toString).getOrElse(""))
        attrRow.getCell(10).setCellType(CellType.NUMERIC)
        attrRow.createCell(11).setCellValue(attr.trim.map(_.toString).getOrElse(""))

      }

    }

    xlsOut.delete(true)
    workbook.write(xlsOut.newFileOutputStream(false))
  }

}
