package ai.starlake.schema.generator

import better.files.File
import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Attribute, BigQuerySink, Domain, Format, Partition}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}

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

  private def linearize(attrs: List[Attribute], prefix: List[String] = Nil): List[Attribute] = {
    def finalName(attr: Attribute, prefix: List[String]) = {
      prefix match {
        case Nil => attr.name
        case _   => prefix.mkString(".") + "." + attr.name
      }
    }

    attrs.flatMap { attr =>
      attr.`type` match {
        case "struct" =>
          val attrList = linearize(attr.attributes.getOrElse(Nil), prefix :+ attr.name)
          attr.copy(name = finalName(attr, prefix)) +: attrList
        case _ =>
          List(attr.copy(name = finalName(attr, prefix)))
      }

    }
  }

  def writeDomainXls(domain: Domain, folder: String): Unit = {
    val workbook = new XSSFWorkbook()
    val font = workbook.createFont
    font.setFontHeightInPoints(14.toShort)
    font.setFontName("Calibri")
    font.setBold(true)
    val boldStyle = workbook.createCellStyle()
    boldStyle.setFont(font)

    def fillHeaders(headers: List[(String, String)], sheet: XSSFSheet): Unit = {
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

    val xlsOut = File(folder, domain.name + ".xlsx")
    val domainSheet = workbook.createSheet("_domain")
    fillHeaders(allDomainHeaders, domainSheet)
    val domainRow = domainSheet.createRow(2)
    domainRow.createCell(0).setCellValue(domain.name)
    domainRow.createCell(1).setCellValue(domain.resolveDirectory())
    domainRow.createCell(2).setCellValue(domain.resolveAck().getOrElse(""))
    domainRow.createCell(3).setCellValue(domain.comment.getOrElse(""))
    domainRow.createCell(4).setCellValue(domain.tableRefs.getOrElse(Nil).mkString(","))
    domainRow.createCell(5).setCellValue(domain.rename.getOrElse(""))
    for (i <- allDomainHeaders.indices)
      domainSheet.autoSizeColumn(i)

    val policySheet = workbook.createSheet("_policies")
    fillHeaders(allPolicyHeaders, policySheet)
    domain.policies().zipWithIndex.foreach { case (policy, rowIndex) =>
      val policyRow = policySheet.createRow(2 + rowIndex)
      policyRow.createCell(0).setCellValue(policy.name)
      policyRow.createCell(1).setCellValue(policy.predicate)
      policyRow.createCell(2).setCellValue(policy.grants.mkString(","))
      policyRow.createCell(3).setCellValue(policy.description)
    }
    for (i <- allPolicyHeaders.indices)
      policySheet.autoSizeColumn(i)

    val schemaSheet = workbook.createSheet("_schemas")
    fillHeaders(allSchemaHeaders, schemaSheet)
    domain.tables.zipWithIndex.foreach { case (schema, rowIndex) =>
      val metadata = schema.mergedMetadata(domain.metadata)
      val schemaRow = schemaSheet.createRow(2 + rowIndex)
      val schemaName =
        if (schema.name.length > 31) schema.name.take(27) + "_" + rowIndex else schema.name
      schemaRow.createCell(0).setCellValue(schemaName)
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

      val partitionColumns = metadata.getSink() match {
        case Some(bq: BigQuerySink) =>
          bq.timestamp
            .map(timestamp => Some(Partition(None, Some(List(timestamp)))))
            .getOrElse(metadata.partition)
        case _ => metadata.partition
      }
      schemaRow
        .createCell(12)
        .setCellValue(partitionColumns.map(_.getAttributes().mkString(",")).getOrElse(""))
      schemaRow
        .createCell(13)
        .setCellValue(metadata.getSink().map(_.`type`).getOrElse(""))
      schemaRow
        .createCell(14)
        .setCellValue(metadata.clustering.map(_.mkString(",")).getOrElse(""))
      schemaRow
        .createCell(16)
        .setCellValue(schema.presql.map(_.mkString("###")).getOrElse(""))
      schemaRow
        .createCell(17)
        .setCellValue(schema.postsql.map(_.mkString("###")).getOrElse(""))
      schemaRow
        .createCell(18)
        .setCellValue(schema.primaryKey.map(_.mkString(",")).getOrElse(""))
      schemaRow
        .createCell(19)
        .setCellValue(schema.tags.map(_.mkString(",")).getOrElse(""))
      schemaRow.createCell(20).setCellValue(schema.rename.getOrElse(""))
      if (schema.name.length > 31) schemaRow.createCell(21).setCellValue(schema.name)
      val tablePolicies =
        schema.acl.getOrElse(Nil).map(_.role) ++ schema.rls.getOrElse(Nil).map(_.name)
      schemaRow
        .createCell(22)
        .setCellValue(tablePolicies.mkString(","))

      for (i <- allSchemaHeaders.indices)
        schemaSheet.autoSizeColumn(i)

      val attributesSheet = workbook.createSheet(schemaName)
      fillHeaders(allAttributeHeaders, attributesSheet)
      linearize(schema.attributes).zipWithIndex.foreach { case (attr, rowIndex) =>
        val attrRow = attributesSheet.createRow(2 + rowIndex)
        val finalName = if (attr.array.getOrElse(false)) attr.name + '*' else attr.name
        attrRow.createCell(0).setCellValue(finalName)
        attrRow.createCell(1).setCellValue(attr.rename.getOrElse(""))
        attrRow.createCell(2).setCellValue(attr.`type`)
        attrRow.createCell(3).setCellValue(attr.required)
        attrRow.getCell(3).setCellType(CellType.BOOLEAN)
        attrRow.createCell(4).setCellValue(attr.getPrivacy().toString)
        attrRow.createCell(5).setCellValue(attr.metricType.map(_.toString).getOrElse(""))
        attrRow.createCell(6).setCellValue(attr.default.getOrElse(""))
        attrRow.createCell(7).setCellValue(attr.script.getOrElse(""))
        attrRow.createCell(8).setCellValue(attr.comment.getOrElse(""))
        attrRow.createCell(9).setCellValue("")
        attrRow.createCell(10).setCellValue("")
        if (metadata.format.getOrElse(Format.DSV) == Format.POSITION) {
          attrRow.getCell(9).setCellType(CellType.NUMERIC)
          attrRow.getCell(10).setCellType(CellType.NUMERIC)
          attrRow.getCell(9).setCellValue(attr.position.map(_.first.toString).getOrElse(""))
          attrRow.getCell(10).setCellValue(attr.position.map(_.last.toString).getOrElse(""))
        }
        attrRow.createCell(11).setCellValue(attr.trim.map(_.toString).getOrElse(""))
        attrRow.createCell(12).setCellValue(attr.ignore.map(_.toString).getOrElse("false"))
        attrRow.createCell(13).setCellValue(attr.foreignKey.getOrElse(""))
        attrRow.createCell(14).setCellValue(attr.tags.map(_.mkString(",")).getOrElse(""))
        attrRow.createCell(15).setCellValue(attr.accessPolicy.getOrElse(""))
      }
      for (i <- allAttributeHeaders.indices)
        attributesSheet.autoSizeColumn(i)

    }

    xlsOut.delete(swallowIOExceptions = true)
    workbook.write(xlsOut.newFileOutputStream(false))
  }

}
