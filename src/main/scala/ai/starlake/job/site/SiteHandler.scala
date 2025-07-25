package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.core.utils.StringUtils
import ai.starlake.lineage._
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{AutoJobInfo, AutoTaskInfo, DomainInfo, SchemaInfo}
import ai.starlake.sql.SQLUtils
import better.files.File
import com.manticore.jsqlformatter.JSQLFormatter
import com.typesafe.scalalogging.LazyLogging
import org.fusesource.scalate.{TemplateEngine, TemplateSource}

import scala.util.Try

class SiteHandler(config: SiteConfig, schemaHandler: SchemaHandler)(implicit val settings: Settings)
    extends LazyLogging {
  def run(): Try[Unit] = Try {
    config.outputPath.createDirectoryIfNotExists()
    buildDomains(config)
    buildJobs(config)
  }

  def buildDomains(config: SiteConfig) = {
    var domainIndex = 1

    val domainPaths = config.outputPath / "1000.load"
    domainPaths.delete(swallowIOExceptions = true)
    domainPaths.createDirectoryIfNotExists()
    val category =
      s"""
         |{
         |  "label": "Load",
         |  "link": {
         |    "type": "generated-index",
         |    "description": "${schemaHandler
          .domains()
          .size} domain(s) with ${schemaHandler.domains().flatMap(_.tables).size} table(s)"
         |
         |  }
         |}
         |""".stripMargin
    (domainPaths / "_category_.json").writeText(category)
    schemaHandler.domains().sortBy(_.finalName).foreach { domain =>
      buildDomain(domainPaths, domainIndex, domain, config)
      domainIndex = domainIndex + 1
    }
  }

  private def buildDomain(
    domainPath: File,
    domainIndex: Int,
    domain: DomainInfo,
    config: SiteConfig
  ): Unit = {
    val normalizedDomainName = StringUtils.replaceNonAlphanumericWithUnderscore(domain.finalName)
    val formattedDomainIndex = "%04d".format(domainIndex)
    val domainFolder = File(domainPath, formattedDomainIndex + "." + normalizedDomainName)
    domainFolder.createDirectoryIfNotExists()
    val category =
      s"""
         |{
         |  "label": "${domain.finalName}",
         |  "link": {
         |    "type": "generated-index",
         |    "description": "${domain.comment
          .map(_.replaceAll("\"", "'"))
          .getOrElse("Description not provided")}"
         |  }
         |}
         |""".stripMargin
    File(domainFolder, "_category_.json").writeText(category)
    var tableIndex = 1
    domain.tables.sortBy(_.finalName).foreach { table =>
      val formattedIndex = "%04d".format(tableIndex)
      tableIndex = tableIndex + 1
      val tableFolder = File(domainFolder, s"$formattedIndex.${table.finalName}.mdx")
      buildTable(domain, table, tableFolder, config)
    }
  }

  private def buildTable(
    domain: DomainInfo,
    schema: SchemaInfo,
    tableFile: File,
    config: SiteConfig
  ): Unit = {
    applyTableSSPAndSave(
      domain,
      tableFile,
      schema,
      config
    )
  }

  def buildJobs(config: SiteConfig): Unit = {
    var jobIndex = 1
    val jobPaths = config.outputPath / "1100.transform"
    jobPaths.delete(swallowIOExceptions = true)
    jobPaths.createDirectoryIfNotExists()
    val category =
      s"""
         |{
         |  "label": "Transform",
         |  "link": {
         |    "type": "generated-index",
         |    "description": "${schemaHandler
          .jobs()
          .size} domain(s) with ${schemaHandler.jobs().flatMap(_.tasks).size} table(s)"
         |
         |  }
         |}
         |""".stripMargin
    (jobPaths / "_category_.json").writeText(category)
    schemaHandler.jobs().sortBy(_.name).foreach { job =>
      buildJob(jobPaths, jobIndex, job, config)
      jobIndex = jobIndex + 1
    }
  }

  private def buildJob(
    jobPath: File,
    jobIndex: Int,
    jobDesc: AutoJobInfo,
    config: SiteConfig
  ): Unit = {
    val normalizedJobName = StringUtils.replaceNonAlphanumericWithUnderscore(jobDesc.name)
    val formattedJobIndex = "%04d".format(jobIndex)
    val jobFolder = File(jobPath, formattedJobIndex + "." + normalizedJobName)
    jobFolder.createDirectoryIfNotExists()
    val category =
      s"""
         |{
         |  "label": "${jobDesc.name}",
         |  "link": {
         |    "type": "generated-index",
         |    "description": "${jobDesc.comment.getOrElse("Description not provided")}"
         |  }
         |}
         |""".stripMargin
    File(jobFolder, "_category_.json").writeText(category)
    var taskIndex = 1
    jobDesc.tasks.foreach { task =>
      val formattedIndex = "%04d".format(taskIndex)
      taskIndex = taskIndex + 1
      val tableFolder = File(jobFolder, s"$formattedIndex.${task.name}.mdx")
      buildTask(jobDesc, task, tableFolder, config)
    }
  }

  private def buildTask(
    jobDesc: AutoJobInfo,
    taskDesc: AutoTaskInfo,
    taskFile: File,
    config: SiteConfig
  ) = {
    applyTaskSSPAndSave(
      jobDesc,
      taskDesc,
      taskFile,
      config
    )
  }

  private def buildTableSVG(relationsSVGFile: File, tables: List[String]): Unit = {
    val config = new TableDependenciesConfig(
      includeAllAttributes = false,
      related = true,
      outputFile = Some(relationsSVGFile),
      tables = Some(tables),
      reload = false,
      svg = true
    )

    val service = new TableDependencies(schemaHandler)
    service.relationsAsDotFile(config)
  }

  private def buildTaskSVG(outputFile: File, tasks: List[String]): Unit = {
    val config =
      AutoTaskDependenciesConfig(
        outputFile = Some(outputFile),
        tasks = Some(tasks),
        viz = true,
        svg = true
      )

    val service = new AutoTaskDependencies(settings, schemaHandler, settings.storageHandler())
    service.jobAsDot(config)
  }

  private def buildACLSVG(aclSVGFile: File, tables: List[String]): Unit = {
    val config = new AclDependenciesConfig(
      outputFile = Some(aclSVGFile),
      tables = tables,
      reload = false,
      svg = true,
      all = true
    )

    val service = new AclDependencies(schemaHandler)
    service.aclsAsDotFile(config)
  }

  lazy val sspEngine: TemplateEngine = new TemplateEngine()

  def applyTableSSPAndSave(
    domain: DomainInfo,
    outputFile: File,
    table: SchemaInfo,
    config: SiteConfig
  ): Unit = {
    val relationsSVGFile = File(outputFile.parent, table.finalName + "-relations.svg")
    val relationsSVG =
      buildTableSVG(relationsSVGFile, List(s"${domain.finalName}.${table.finalName}"))

    val aclSVGFile = File(outputFile.parent, table.finalName + "-acl.svg")
    val aclSVG = buildACLSVG(aclSVGFile, List(s"${domain.finalName}.${table.finalName}"))
    val paramMap = Map(
      "table"         -> table,
      "schemaHandler" -> schemaHandler,
      "relationsSVG"  -> relationsSVGFile.name,
      "aclSVG"        -> aclSVGFile.name
    )

    val (sspResource, templateContent) = config.templateContent(SiteCmd.TABLE_TEMPLATE)

    val sspOutput = sspEngine.layout(
      TemplateSource.fromText(sspResource, templateContent),
      paramMap
    )
    outputFile.writeText(sspOutput)
  }

  def applyTaskSSPAndSave(
    jobDesc: AutoJobInfo,
    taskDesc: AutoTaskInfo,
    outputFile: File,
    config: SiteConfig
  ): Unit = {
    val relationsSVGFile = File(outputFile.parent, taskDesc.name + "-relations.svg")
    buildTaskSVG(relationsSVGFile, List(taskDesc.name))

    val aclSVGFile = File(outputFile.parent, taskDesc.name + "-acl.svg")
    val aclSVG = buildACLSVG(aclSVGFile, List(s"${jobDesc.name}.${taskDesc.name}"))
    val sql = taskDesc.sql.getOrElse("")
    val formattedSql =
      SQLUtils.format(sql, JSQLFormatter.OutputFormat.PLAIN)
    val paramMap = Map(
      "task"          -> taskDesc,
      "schemaHandler" -> schemaHandler,
      "relationsSVG"  -> relationsSVGFile.name,
      "aclSVG"        -> aclSVGFile.name,
      "sql"           -> formattedSql,
      "python"        -> taskDesc.python.getOrElse("")
    )

    val (sspResource, templateContent) = config.templateContent(SiteCmd.TASK_TEMPLATE)
    val sspOutput = sspEngine.layout(
      TemplateSource.fromText(sspResource, templateContent),
      paramMap
    )
    outputFile.writeText(sspOutput)
  }
}
