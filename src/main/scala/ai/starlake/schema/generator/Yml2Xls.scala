package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import scala.util.Try

class Yml2Xls(schemaHandler: SchemaHandler) extends LazyLogging with XlsModel {

  def run(args: Array[String]): Try[Unit] = {
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    Yml2XlsCmd.run(args, schemaHandler).map(_ => ())
  }

  def generateXls(domainNames: Seq[String], outputDir: String)(implicit
    settings: Settings
  ): Unit = {
    val domains =
      domainNames match {
        case Nil => schemaHandler.domains(reload = true)
        case x   => schemaHandler.domains().filter(domain => x.contains(domain.name))
      }
    if (domains.isEmpty) {
      val existingDomainNames = schemaHandler.domains().map(_.name)
      throw new Exception(
        s"[${domainNames.mkString(",")}] not found in projects domains : [${existingDomainNames.mkString(",")}]"
      )
    }

    domains.foreach { domain =>
      writeDomainXls(domain, outputDir)
    }

    schemaHandler
      .iamPolicyTags()
      .foreach(iamPolicyTags =>
        Yml2XlsIamPolicyTags.writeXls(iamPolicyTags, DatasetArea.metadata.toString)
      )
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
          val attrList = linearize(attr.attributes, prefix :+ attr.name)
          attr.copy(name = finalName(attr, prefix)) +: attrList
        case _ =>
          List(attr.copy(name = finalName(attr, prefix)))
      }

    }
  }

  def writeDomainXls(domain: Domain, folder: String): Unit = {
    val workbook = new XSSFWorkbook()

    val xlsOut = File(folder, domain.name + ".xlsx")
    val domainSheet = workbook.createSheet("_domain")
    fillHeaders(workbook, allDomainHeaders, domainSheet)
    val domainRow = domainSheet.createRow(2)
    domainRow.createCell(0).setCellValue(domain.name)
    domainRow.createCell(1).setCellValue(domain.resolveDirectoryOpt().getOrElse(""))
    domainRow.createCell(2).setCellValue(domain.resolveAck().getOrElse(""))
    domainRow.createCell(3).setCellValue(domain.comment.getOrElse(""))
    domainRow.createCell(4).setCellValue(domain.rename.getOrElse(""))
    for (i <- allDomainHeaders.indices)
      domainSheet.autoSizeColumn(i)

    val policySheet = workbook.createSheet("_policies")
    fillHeaders(workbook, allPolicyHeaders, policySheet)
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
    fillHeaders(workbook, allSchemaHeaders, schemaSheet)
    domain.tables.zipWithIndex.foreach { case (schema, rowIndex) =>
      val metadata = schema.mergedMetadata(domain.metadata)
      val schemaRow = schemaSheet.createRow(2 + rowIndex)
      val schemaName =
        if (schema.name.length > 31) schema.name.take(27) + "_" + rowIndex else schema.name
      schemaRow.createCell(0).setCellValue(schemaName)
      schemaRow.createCell(1).setCellValue(schema.pattern.toString)
      schemaRow.createCell(2).setCellValue(metadata.mode.map(_.toString).getOrElse(""))
      if (schema.getStrategy().`type` == StrategyType.SCD2)
        schemaRow.createCell(3).setCellValue(StrategyType.SCD2.value)
      else
        schemaRow.createCell(3).setCellValue(metadata.write.map(_.toString).getOrElse(""))
      schemaRow.createCell(4).setCellValue(metadata.format.map(_.toString).getOrElse(""))
      schemaRow.createCell(5).setCellValue(metadata.withHeader.map(_.toString).getOrElse(""))
      schemaRow.getCell(5).setCellType(CellType.BOOLEAN)
      schemaRow.createCell(6).setCellValue(metadata.getSeparator())

      schema.strategy.foreach { mergeOptions =>
        schemaRow.createCell(7).setCellValue(mergeOptions.timestamp.getOrElse(""))
        schemaRow.createCell(8).setCellValue(mergeOptions.key.mkString(","))
        schemaRow.createCell(15).setCellValue(mergeOptions.queryFilter.getOrElse(""))
      }
      schemaRow.createCell(9).setCellValue(schema.comment.getOrElse(""))
      schemaRow.createCell(10).setCellValue(metadata.encoding.getOrElse(""))
      schemaRow
        .createCell(11)
        .setCellValue("")

      val partitionColumns = metadata.sink
        .flatMap(
          _.timestamp.map(timestamp => Partition(List(timestamp)))
        )
        .orElse {
          metadata.sink.flatMap(_.partition)
        }

      val clusteringColumns = metadata.sink match {
        case Some(sink) =>
          sink.clustering.getOrElse(Nil)
        case _ => Nil
      }
      schemaRow
        .createCell(12)
        .setCellValue(partitionColumns.map(_.getAttributes().mkString(",")).getOrElse(""))
      schemaRow
        .createCell(13)
        .setCellValue("FS")
      schemaRow
        .createCell(14)
        .setCellValue(clusteringColumns.mkString(","))
      schemaRow
        .createCell(16)
        .setCellValue(schema.presql.mkString("###"))
      schemaRow
        .createCell(17)
        .setCellValue(schema.postsql.mkString("###"))
      schemaRow
        .createCell(18)
        .setCellValue(schema.primaryKey.mkString(","))
      schemaRow
        .createCell(19)
        .setCellValue(schema.tags.mkString(","))
      schemaRow.createCell(20).setCellValue(schema.rename.getOrElse(""))
      if (schema.name.length > 31) schemaRow.createCell(21).setCellValue(schema.name)
      val tablePolicies =
        schema.acl.map(_.role) ++ schema.rls.map(_.name)
      schemaRow
        .createCell(22)
        .setCellValue(tablePolicies.mkString(","))

      for (i <- allSchemaHeaders.indices)
        schemaSheet.autoSizeColumn(i)

      val attributesSheet = workbook.createSheet(schemaName)
      fillHeaders(workbook, allAttributeHeaders, attributesSheet)
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
        attrRow.createCell(14).setCellValue(attr.tags.mkString(","))
        attrRow.createCell(15).setCellValue(attr.accessPolicy.getOrElse(""))
      }
      for (i <- allAttributeHeaders.indices)
        attributesSheet.autoSizeColumn(i)

    }

    xlsOut.delete(swallowIOExceptions = true)
    workbook.write(xlsOut.newFileOutputStream(false))
  }

}
