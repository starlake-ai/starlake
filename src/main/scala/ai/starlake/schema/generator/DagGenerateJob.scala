package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{JDBCSchema, JDBCTable}
import ai.starlake.lineage.{AutoTaskDependencies, AutoTaskDependenciesConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Severity, _}
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.util
import scala.jdk.CollectionConverters._
import scala.util.Try

class DagGenerateJob(schemaHandler: SchemaHandler) extends LazyLogging {

  def run(args: Array[String])(implicit settings: Settings): Try[Unit] =
    DagGenerateCmd.run(args.toIndexedSeq, schemaHandler).map(_ => ())

  case class TableWithDagConfig(
    domain: Domain,
    table: Schema,
    dagConfigName: String,
    dagConfig: DagGenerationConfig,
    schedule: Option[String]
  )

  private def tableWithDagConfigs(
    dagConfigs: Map[String, DagGenerationConfig],
    tags: Set[String]
  )(implicit settings: Settings): List[TableWithDagConfig] = {
    logger.info("Starting to generate dags")
    val tableWithDagConfigAndSchedule = schemaHandler.domains().flatMap { domain =>
      domain.tables.filter(tags.isEmpty || _.tags.intersect(tags).nonEmpty).flatMap { table =>
        val mergedMetadata = table.mergedMetadata(domain.metadata)
        val dagConfigRef = mergedMetadata.dagRef
          .orElse(settings.appConfig.dagRef.flatMap(_.load))
        val schedule = mergedMetadata.schedule

        dagConfigRef.map { dagRef =>
          val dagConfig = dagConfigs.getOrElse(
            dagRef,
            throw new Exception(
              s"Could not find dag config $dagRef referenced in ${domain.name}.${table.name}. Dag configs found ${dagConfigs.keys
                  .mkString(",")}"
            )
          )
          TableWithDagConfig(domain, table, dagRef, dagConfig, schedule)
        }
      }
    }
    tableWithDagConfigAndSchedule
  }

  case class ExtractedTableWithDagConfig(
    configFile: String,
    schema: JDBCSchema,
    table: Option[JDBCTable],
    dagConfigName: String,
    dagConfig: DagGenerationConfig,
    schedule: Option[String]
  )

  private def extractedTableWithDagConfigs(
    dagConfigs: Map[String, DagGenerationConfig],
    tags: Set[String]
  )(implicit settings: Settings): List[ExtractedTableWithDagConfig] = {
    logger.info("Starting to generate dags")
    val extractedTableWithDagConfigAndSchedule =
      schemaHandler.loadExtractConfigs().flatMap { configs =>
        val configFile = configs._1
        configs._2.jdbcSchemas.flatMap { schema =>
          if (schema.tables.isEmpty) {
            if (tags.isEmpty || schema.tags.intersect(tags).nonEmpty) {
              val dagConfigRef = schema.dagRef
                .orElse(settings.appConfig.dagRef.flatMap(_.load))
              dagConfigRef.map { dagRef =>
                val dagConfig = dagConfigs.getOrElse(
                  dagRef,
                  throw new Exception(
                    s"Could not find dag config $dagRef referenced in ${schema.schema}. Dag configs found ${dagConfigs.keys
                        .mkString(",")}"
                  )
                )
                ExtractedTableWithDagConfig(
                  configFile,
                  schema,
                  None,
                  dagRef,
                  dagConfig,
                  schema.schedule
                )
              }
            } else {
              None
            }
          } else {
            schema.tables.filter(tags.isEmpty || _.tags.intersect(tags).nonEmpty).flatMap { table =>
              val dagConfigRef = table.dagRef
                .orElse(schema.dagRef)
                .orElse(settings.appConfig.dagRef.flatMap(_.load))
              val schedule = table.schedule.orElse(schema.schedule)
              dagConfigRef.map { dagRef =>
                val dagConfig = dagConfigs.getOrElse(
                  dagRef,
                  throw new Exception(
                    s"Could not find dag config $dagRef referenced in ${schema.schema}.${table.name}. Dag configs found ${dagConfigs.keys
                        .mkString(",")}"
                  )
                )
                ExtractedTableWithDagConfig(
                  configFile,
                  schema,
                  Some(table),
                  dagRef,
                  dagConfig,
                  schedule
                )
              }
            }
          }
        }
      }
    extractedTableWithDagConfigAndSchedule.toList
  }

  case class TaskWithDagConfig(
    taskDesc: AutoTaskDesc,
    dagConfigName: String,
    dagConfig: DagGenerationConfig,
    schedule: Option[String]
  )

  private def taskWithDagConfigs(
    dagConfigs: Map[String, DagGenerationConfig],
    tags: Set[String]
  )(implicit settings: Settings): List[TaskWithDagConfig] = {
    logger.info("Starting to task generate dags")
    val tasks = schemaHandler.tasks()
    val taskWithDagConfigAndSchedule: List[TaskWithDagConfig] =
      tasks.filter(tags.isEmpty || _.tags.intersect(tags).nonEmpty).flatMap { taskWithDag =>
        val dagConfigRef = taskWithDag.dagRef
          .orElse(settings.appConfig.dagRef.flatMap(_.transform))
        val schedule = taskWithDag.schedule
        dagConfigRef.map { dagRef =>
          val dagConfig = dagConfigs.getOrElse(
            dagRef,
            throw new Exception(
              s"Could not find dag config $dagRef referenced in ${taskWithDag.name}. Dag config found ${dagConfigs.keys
                  .mkString(",")}"
            )
          )
          TaskWithDagConfig(taskWithDag, dagRef, dagConfig, schedule)
        }
      }
    taskWithDagConfigAndSchedule
  }

  def getScheduleName(schedule: String, currentScheduleIndex: Int): (String, Int) = {
    if (schedule.contains(" ")) {
      ("cron" + currentScheduleIndex.toString, currentScheduleIndex + 1)
    } else {
      (schedule, currentScheduleIndex)
    }
  }

  def generateTaskDags(
    config: DagGenerateConfig
  )(implicit settings: Settings): Unit = {
    val outputDir = new Path(
      config.outputDir.getOrElse(DatasetArea.dags.toString + "/generated/")
    )

    val dagConfigs = schemaHandler.loadDagGenerationConfigs()
    val taskConfigs = taskWithDagConfigs(dagConfigs, config.tags.toSet)
    val env = schemaHandler.activeEnvVars(root = Option(settings.appConfig.root))
    val jEnv = env.map { case (k, v) =>
      DagPair(k, v)
    }.toList
    val depsEngine = new AutoTaskDependencies(settings, schemaHandler, settings.storageHandler())
    val taskConfigsGroupedByFilename = taskConfigs
      .map { taskConfig =>
        val envVars = schemaHandler.activeEnvVars(root = Option(settings.appConfig.root)) ++ Map(
          "table"    -> taskConfig.taskDesc.table,
          "domain"   -> taskConfig.taskDesc.domain,
          "name"     -> taskConfig.taskDesc.name, // deprecated
          "task"     -> taskConfig.taskDesc.name,
          "schedule" -> taskConfig.schedule.getOrElse("None")
        )
        val filename = Utils.parseJinja(
          taskConfig.dagConfig.filename,
          envVars
        )
        val comment = Utils.parseJinja(
          taskConfig.dagConfig.comment,
          envVars
        )
        val options =
          taskConfig.dagConfig.options.map { case (k, v) => k -> Utils.parseJinja(v, envVars) }
        (
          filename,
          taskConfig
            .copy(dagConfig = taskConfig.dagConfig.copy(options = options, comment = comment))
        )
      }
      .groupBy { case (filename, _) => filename }
    taskConfigsGroupedByFilename
      .foreach { case (filename, taskConfigs) =>
        val headConfig = taskConfigs.head match { case (_, config) => config }
        val dagConfig = headConfig.dagConfig
        val dagTemplateName = dagConfig.template
        val dagTemplateContent = new Yml2DagTemplateLoader().loadTemplate(dagTemplateName)
        val cron = settings.appConfig.schedulePresets.getOrElse(
          headConfig.schedule.getOrElse("None"),
          headConfig.schedule.getOrElse("None")
        )
        val cronIfNone = if (cron == "None") null else cron
        val configs = taskConfigs.map { case (_, config) => config.taskDesc.name }
        val config = AutoTaskDependenciesConfig(tasks = Some(configs))
        val deps = depsEngine.jobsDependencyTree(config)
        val context = TransformDagGenerationContext(
          config = dagConfig,
          deps = deps,
          cron = Option(cronIfNone)
        )
        applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
      }
  }

  def clean(dir: Option[String])(implicit settings: Settings): Unit = {
    val outputDir = new Path(
      dir.getOrElse(DatasetArea.dags.toString + "/generated/")
    )
    settings.storageHandler().mkdirs(outputDir)
    logger.info(s"Cleaning output directory $outputDir")
    settings.storageHandler().delete(outputDir)
  }

  def generateDomainDags(
    config: DagGenerateConfig
  )(implicit settings: Settings): Unit = {
    val outputDir = new Path(
      config.outputDir.getOrElse(DatasetArea.dags.toString + "/generated/")
    )

    val dagConfigs = schemaHandler.loadDagGenerationConfigs()
    val tableConfigs = tableWithDagConfigs(dagConfigs, config.tags.toSet)
    val groupedDags = groupByDagConfigScheduleDomain(tableConfigs)

    val extractedTableConfigs = extractedTableWithDagConfigs(dagConfigs, config.tags.toSet)
    val extractedGroupedDags = groupByExtractedDagConfigScheduleDomain(extractedTableConfigs)

    val env = schemaHandler.activeEnvVars(root = Some(settings.appConfig.root))
    val jEnv = env.map { case (k, v) =>
      DagPair(k, v)
    }.toList

    groupedDags.foreach { case (dagConfigName, groupedBySchedule) =>
      val dagConfig = dagConfigs(dagConfigName)
      val errors = dagConfig.checkValidity().filter(_.severity == Severity.Error)
      errors.foreach { error =>
        logger.error(error.toString())
      }
      if (errors.nonEmpty) {
        throw new Exception(s"Dag config ${dagConfigName} is invalid: ${errors.mkString(",")}")
      }
      val extractedGroupedBySchedule = extractedGroupedDags.getOrElse(dagConfigName, Map.empty)
      val dagTemplateName = dagConfig.template
      val dagTemplateContent = new Yml2DagTemplateLoader().loadTemplate(dagTemplateName)
      val filenameVars = dagConfig.getfilenameVars()
      if (filenameVars.exists(List("table", "finalTable", "renamedTable").contains)) {
        if (!filenameVars.exists(List("domain", "finalDomain", "renamedDomain").contains))
          logger.warn(
            s"Dag Config $dagConfigName: filename contains table but not domain, this will generate multiple dags with the same name if the same table name appear in multiple domains"
          )
        // one dag per table
        var scheduleIndex = 1
        groupedBySchedule.foreach { case (schedule, groupedByDomain) =>
          groupedByDomain.foreach { case (domain, tables) =>
            tables.foreach { table =>
              val dagDomain = DagDomain(
                domain.name,
                domain.finalName,
                java.util.List.of[TableDomain](TableDomain(table.name, table.finalName))
              )
              val (scheduleValue, nextScheduleIndex) = getScheduleName(schedule, scheduleIndex)
              val cron = settings.appConfig.schedulePresets.getOrElse(
                schedule,
                scheduleValue
              )
              val cronIfNone = if (cron == "None") null else cron
              val extractedGroupedByConfigFile =
                extractedGroupedBySchedule.getOrElse(schedule, Map.empty)
              val dagExtractions = extractedGroupedByConfigFile
                .map { case (configFile, groupedBySchema) =>
                  val dagSchemas = groupedBySchema
                    .flatMap {
                      case (schema, tables)
                          if schema.schema == domain.name && tables.exists(_.name == table.name) =>
                        Some(
                          DagSchema(
                            schema.schema,
                            List(TableSchema(table.name)).asJava
                          )
                        )
                      case _ => None
                    }
                    .toList
                    .sortBy(_.name)
                  DagExtraction(
                    configFile,
                    dagSchemas.asJava
                  )
                }
                .toList
                .sortBy(_.config)
              val schedules =
                List(
                  DagSchedule(
                    schedule,
                    cronIfNone,
                    java.util.List.of[DagDomain](dagDomain),
                    dagExtractions.asJava
                  )
                )

              val envVars =
                schemaHandler.activeEnvVars(root = Option(settings.appConfig.root)) ++ Map(
                  "schedule"      -> scheduleValue,
                  "domain"        -> domain.name,
                  "finalDomain"   -> domain.finalName,
                  "renamedDomain" -> domain.finalName,
                  "table"         -> table.name,
                  "finalTable"    -> table.finalName,
                  "renamedTable"  -> table.finalName
                )
              val options = dagConfig.options.map { case (k, v) =>
                k -> Utils.parseJinja(v, envVars)
              }
              val comment = Utils.parseJinja(dagConfig.comment, envVars)
              val context = LoadDagGenerationContext(
                config = dagConfig.copy(options = options, comment = comment),
                schedules
              )

              scheduleIndex = nextScheduleIndex
              val filename = Utils.parseJinja(dagConfig.filename, envVars)
              applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
            }
          }
        }
      } else if (filenameVars.exists(List("domain", "finalDomain", "renamedDomain").contains)) {
        // one dag per domain
        val domains = groupedBySchedule
          .flatMap { case (_, groupedByDomain) =>
            groupedByDomain.keys
          }
          .toList
          .sortBy(_.name)
        domains.foreach { domain =>
          val schedules = groupedBySchedule
            .flatMap { case (schedule, groupedByDomain) =>
              val tables = groupedByDomain.get(domain)
              tables.map { table =>
                val dagDomain = DagDomain(
                  domain.name,
                  domain.finalName,
                  table.map(t => TableDomain(t.name, t.finalName)).asJava
                )
                val cron = settings.appConfig.schedulePresets.getOrElse(
                  schedule,
                  schedule
                )
                val cronIfNone = if (cron == "None") null else cron
                val extractedGroupedByConfigFile =
                  extractedGroupedBySchedule.getOrElse(schedule, Map.empty)
                val dagExtractions = extractedGroupedByConfigFile
                  .map { case (configFile, groupedBySchema) =>
                    val dagSchemas = groupedBySchema
                      .flatMap {
                        case (schema, tables) if schema.schema == domain.name =>
                          Some(
                            DagSchema(
                              schema.schema,
                              tables.map(t => TableSchema(t.name)).asJava
                            )
                          )
                        case _ => None
                      }
                      .toList
                      .sortBy(_.name)
                    DagExtraction(
                      configFile,
                      dagSchemas.asJava
                    )
                  }
                  .toList
                  .sortBy(_.config)
                DagSchedule(
                  schedule,
                  cronIfNone,
                  java.util.List.of[DagDomain](dagDomain),
                  dagExtractions.asJava
                )
              }
            }
            .toList
            .sortBy(_.schedule)
          if (filenameVars.contains("schedule")) {
            var scheduleIndex = 1
            schedules.foreach { schedule =>
              val (scheduleValue, nextScheduleIndex) =
                getScheduleName(schedule.schedule, scheduleIndex)
              scheduleIndex = nextScheduleIndex
              val envVars =
                schemaHandler.activeEnvVars(root = Option(settings.appConfig.root)) ++ Map(
                  "schedule"      -> scheduleValue,
                  "domain"        -> domain.name,
                  "renamedDomain" -> domain.finalName,
                  "finalDomain"   -> domain.finalName
                )
              val options = dagConfig.options.map { case (k, v) =>
                k -> Utils.parseJinja(v, envVars)
              }
              val comment = Utils.parseJinja(dagConfig.comment, envVars)
              val context = LoadDagGenerationContext(
                config = dagConfig.copy(options = options, comment = comment),
                schedules
              )
              val filename = Utils.parseJinja(dagConfig.filename, envVars)
              applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
            }
          } else {
            val envVars =
              schemaHandler.activeEnvVars(root = Option(settings.appConfig.root)) ++ Map(
                "domain"        -> domain.name,
                "renamedDomain" -> domain.finalName,
                "finalDomain"   -> domain.finalName
              )
            val options = dagConfig.options.map { case (k, v) => k -> Utils.parseJinja(v, envVars) }
            val comment = Utils.parseJinja(dagConfig.comment, envVars)
            val context = LoadDagGenerationContext(
              config = dagConfig.copy(options = options, comment = comment),
              schedules
            )
            val filename = Utils.parseJinja(dagConfig.filename, envVars)
            applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
          }
        }
      } else {
        // one dag per config
        var scheduleIndex = 1
        val dagConfig = dagConfigs(dagConfigName)
        val dagTemplateName = dagConfig.template
        val dagTemplateContent = new Yml2DagTemplateLoader().loadTemplate(dagTemplateName)
        val dagSchedules = groupedBySchedule
          .map { case (schedule, groupedByDomain) =>
            val (scheduleValue, nextScheduleIndex) =
              getScheduleName(schedule, scheduleIndex)
            scheduleIndex = nextScheduleIndex
            val dagDomains = groupedByDomain
              .map { case (domain, tables) =>
                DagDomain(
                  domain.name,
                  domain.finalName,
                  tables.map(t => TableDomain(t.name, t.finalName)).asJava
                )
              }
              .toList
              .sortBy(_.name)
            val cron = settings.appConfig.schedulePresets.getOrElse(
              schedule,
              schedule
            )
            val cronIfNone = if (cron == "None") null else cron
            val extractedGroupedByConfigFile =
              extractedGroupedBySchedule.getOrElse(schedule, Map.empty)
            val dagExtractions = extractedGroupedByConfigFile
              .map { case (configFile, groupedBySchema) =>
                val dagSchemas = groupedBySchema
                  .map { case (schema, tables) =>
                    DagSchema(
                      schema.schema,
                      tables.map(t => TableSchema(t.name)).asJava
                    )
                  }
                  .toList
                  .sortBy(_.name)
                DagExtraction(
                  configFile,
                  dagSchemas.asJava
                )
              }
              .toList
              .sortBy(_.config)
            DagSchedule(scheduleValue, cronIfNone, dagDomains.asJava, dagExtractions.asJava)
          }
          .toList
          .sortBy(_.schedule)
        if (filenameVars.contains("schedule")) {
          dagSchedules.foreach { schedule =>
            val envVars =
              schemaHandler.activeEnvVars(root = Option(settings.appConfig.root)) ++ Map(
                "schedule" -> schedule.schedule
              )
            val options = dagConfig.options.map { case (k, v) =>
              k -> Utils.parseJinja(v, envVars)
            }
            val comment = Utils.parseJinja(dagConfig.comment, envVars)
            val context = LoadDagGenerationContext(
              config = dagConfig.copy(options = options, comment = comment),
              List(schedule)
            )
            val filename = Utils.parseJinja(dagConfig.filename, envVars)
            applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
          }
        } else {
          val envVars = schemaHandler.activeEnvVars(root = Option(settings.appConfig.root))
          val options = dagConfig.options.map { case (k, v) => k -> Utils.parseJinja(v, envVars) }
          val comment = Utils.parseJinja(dagConfig.comment, envVars)
          val context = LoadDagGenerationContext(
            config = dagConfig.copy(options = options, comment = comment),
            schedules = dagSchedules
          )
          val filename = Utils.parseJinja(dagConfig.filename, envVars)
          applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
        }
      }
    }
  }

  private def applyJ2AndSave(
    outputDir: Path,
    jEnv: List[DagPair],
    dagTemplateContent: String,
    context: util.HashMap[String, Object],
    filename: String
  )(implicit settings: Settings): Unit = {
    val paramMap = Map(
      "context" -> context,
      "env"     -> jEnv.asJava
    )
    val jinjaOutput = Utils.parseJinjaTpl(dagTemplateContent, paramMap)
    val dagPath = new Path(outputDir, filename)
    logger.info(s"Writing dag to $dagPath")
    settings.storageHandler().write(jinjaOutput, dagPath)(StandardCharsets.UTF_8)
  }

  /** group tables by dag config name, schedule and domain
    *
    * @return
    *   Map[dagConfigName, Map[schedule, Map[domainName, List[tableName]]]]
    */
  private def groupByDagConfigScheduleDomain(
    tableWithDagConfigs: List[TableWithDagConfig]
  ): Map[String, Map[String, Map[Domain, List[Schema]]]] = {
    val groupByDagConfigName = tableWithDagConfigs.groupBy(_.dagConfigName)
    val groupDagConfigNameAndSchedule = groupByDagConfigName.map {
      case (dagConfigName, tableWithDagConfigs) =>
        val groupedBySchedule = tableWithDagConfigs.groupBy(_.schedule.getOrElse("None"))
        val groupedByScheduleAndDomain = groupedBySchedule.map {
          case (scheduleName, tableWithDagConfigs) =>
            val groupedByDomain = tableWithDagConfigs.groupBy(_.domain)
            val groupedTableNames = groupedByDomain.map { case (domain, tableWithConfig) =>
              domain -> tableWithConfig.map(_.table).sortBy(_.name)
            }
            scheduleName -> groupedTableNames
        }
        (dagConfigName, groupedByScheduleAndDomain)
    }
    groupDagConfigNameAndSchedule
  }

  /** group extracted tables by dag config name, schedule, config file and domain
    */
  private def groupByExtractedDagConfigScheduleDomain(
    tableWithDagConfigs: List[ExtractedTableWithDagConfig]
  ): Map[String, Map[String, Map[String, Map[JDBCSchema, List[JDBCTable]]]]] = {
    val groupByDagConfigName = tableWithDagConfigs.groupBy(_.dagConfigName)
    val groupDagConfigNameAndSchedule = groupByDagConfigName.map {
      case (dagConfigName, tableWithDagConfigs) =>
        val groupedBySchedule = tableWithDagConfigs.groupBy(_.schedule.getOrElse("None"))
        val groupedByScheduleAndDomain = groupedBySchedule.map {
          case (scheduleName, tableWithDagConfigs) =>
            val groupedByConfigFile = tableWithDagConfigs.groupBy(_.configFile)
            val groupedConfigFiles = groupedByConfigFile.map { case (configFile, tableWithConfig) =>
              val groupedBySchema = tableWithConfig.groupBy(_.schema)
              val groupedTableNames = groupedBySchema.map { case (schema, tableWithConfig) =>
                schema -> tableWithConfig.flatMap(_.table).sortBy(_.name)
              }
              configFile -> groupedTableNames
            }
            scheduleName -> groupedConfigFiles
        }
        (dagConfigName, groupedByScheduleAndDomain)
    }
    groupDagConfigNameAndSchedule
  }
}

object DagGenerateJob {
  val SCHEDULE = "schedule"
  val name = "generate"
}
