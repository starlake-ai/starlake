package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.schema.generator._
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{AutoJobDesc, AutoTaskDesc, Domain, Schema}
import ai.starlake.utils.Utils
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.fusesource.scalate.{TemplateEngine, TemplateSource}

class SiteHandler(schemaHandler: SchemaHandler)(implicit val settings: Settings)
    extends StrictLogging {
  def run(config: SiteConfig): Unit = {
    config.outputPath.createDirectoryIfNotExists()
    buildDomains(config)
    buildJobs(config)
  }
  def buildDomains(config: SiteConfig) = {
    var domainIndex = 1

    val domainPaths = config.outputPath / "0001.load"
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
    domain: Domain,
    config: SiteConfig
  ): Unit = {
    val normalizedDomainName = Utils.keepAlphaNum(domain.finalName)
    val formattedDomainIndex = "%04d".format(domainIndex)
    val domainFolder = File(domainPath, formattedDomainIndex + "." + normalizedDomainName)
    domainFolder.createDirectoryIfNotExists()
    val category =
      s"""
         |{
         |  "label": "${domain.finalName}",
         |  "link": {
         |    "type": "generated-index",
         |    "description": "${domain.comment.getOrElse("Description not provided")}"
         |  }
         |}
         |""".stripMargin
    File(domainFolder, "_category_.json").writeText(category)
    var tableIndex = 1
    domain.tables.foreach { table =>
      val formattedIndex = "%04d".format(tableIndex)
      tableIndex = tableIndex + 1
      val tableFolder = File(domainFolder, s"$formattedIndex.${table.finalName}.mdx")
      buildTable(domain, table, tableFolder, config)
    }
  }

  private def buildTable(
    domain: Domain,
    schema: Schema,
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
    val jobPaths = config.outputPath / "0002.transform"
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
    jobDesc: AutoJobDesc,
    config: SiteConfig
  ): Unit = {
    val normalizedJobName = Utils.keepAlphaNum(jobDesc.name)
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
    jobDesc: AutoJobDesc,
    taskDesc: AutoTaskDesc,
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

  private def buildTableSVG(tables: List[String]): String = {
    val config = new TableDependenciesConfig(
      includeAllAttributes = false,
      related = true,
      outputFile = None,
      tables = Some(tables),
      reload = false
    )

    val service = new TableDependencies(schemaHandler)
    val svgContent = service.relationsAsDotString(config, svg = true)
    svgContent
  }

  private def buildTaskSVG(tasks: List[String]): String = {
    val config =
      AutoTaskDependenciesConfig(
        tasks = Some(tasks),
        viz = true,
        outputFile = None
      )

    val service = new AutoTaskDependencies(settings, schemaHandler, settings.storageHandler())
    val (jobName, svgContent) = service.jobAsDot(config, svg = true)
    svgContent
  }

  private def buildACLSVG(tables: List[String]): String = {
    val config = new AclDependenciesConfig(
      outputFile = None,
      tables = tables,
      reload = false,
      svg = true
    )

    val service = new AclDependencies(schemaHandler)
    val svgContent = service.aclAsDotString(config)
    svgContent
  }

  lazy val sspEngine: TemplateEngine = new TemplateEngine()

  def applyTableSSPAndSave(
    domain: Domain,
    outputFile: File,
    table: Schema,
    config: SiteConfig
  ): Unit = {
    val relationsSVG = buildTableSVG(List(s"${domain.finalName}.${table.finalName}"))
    val relationsSVGFile = File(outputFile.parent, table.finalName + "-relations.svg")
    relationsSVGFile.writeText(relationsSVG)

    val aclSVG = buildACLSVG(List(s"${domain.finalName}.${table.finalName}"))
    val aclSVGFile = File(outputFile.parent, table.finalName + "-acl.svg")
    aclSVGFile.writeText(aclSVG)
    val paramMap = Map(
      "table"         -> table,
      "schemaHandler" -> schemaHandler,
      "relationsSVG"  -> relationsSVGFile.name,
      "aclSVG"        -> aclSVGFile.name
    )

    val (sspResource, templateContent) = config.templateContent(SiteConfig.TABLE_TEMPLATE)

    val sspOutput = sspEngine.layout(
      TemplateSource.fromText(sspResource, templateContent),
      paramMap
    )
    outputFile.writeText(sspOutput)
  }

  def applyTaskSSPAndSave(
    jobDesc: AutoJobDesc,
    taskDesc: AutoTaskDesc,
    outputFile: File,
    config: SiteConfig
  ): Unit = {
    val relationsSVG = buildTaskSVG(List(taskDesc.name))
    val relationsSVGFile = File(outputFile.parent, taskDesc.name + "-relations.svg")
    relationsSVGFile.writeText(relationsSVG)

    val aclSVG = buildACLSVG(List(s"${jobDesc.name}.${taskDesc.name}"))
    val aclSVGFile = File(outputFile.parent, taskDesc.name + "-acl.svg")
    aclSVGFile.writeText(aclSVG)
    val paramMap = Map(
      "task"          -> taskDesc,
      "schemaHandler" -> schemaHandler,
      "relationsSVG"  -> relationsSVGFile.name,
      "aclSVG"        -> aclSVGFile.name
    )

    val (sspResource, templateContent) = config.templateContent(SiteConfig.TASK_TEMPLATE)
    val sspOutput = sspEngine.layout(
      TemplateSource.fromText(sspResource, templateContent),
      paramMap
    )
    outputFile.writeText(sspOutput)
  }
}
