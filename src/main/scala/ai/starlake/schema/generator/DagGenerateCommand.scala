package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.util
import scala.collection.JavaConverters._
import scala.util.Try

class DagGenerateCommand(schemaHandler: SchemaHandler) extends LazyLogging {

  def run(args: Array[String])(implicit settings: Settings): Try[Unit] = Try {
    DagGenerateConfig.parse(args) match {
      case Some(config) =>
        generateDomainDags(config)
      case _ =>
        throw new IllegalArgumentException(DagGenerateConfig.usage())
    }
  }

  case class TableWithDagConfig(
    domain: Domain,
    table: Schema,
    dagConfigName: String,
    dagConfig: DagGenerationConfig,
    schedule: Option[String]
  )

  private def tableWithDagConfigs(
    dagConfigs: Map[String, DagGenerationConfig]
  )(implicit settings: Settings): List[TableWithDagConfig] = {
    logger.info("Starting to generate dags")
    val tableWithDagConfigAndSchedule = schemaHandler.domains().flatMap { domain =>
      domain.tables.flatMap { table =>
        val mergedMetadata = table.mergedMetadata(domain.metadata)
        val dagConfigRef = mergedMetadata.dagRef
          .orElse(settings.appConfig.dagRef.flatMap(_.load))
        val schedule = mergedMetadata.schedule

        dagConfigRef.map { dagRef =>
          val dagConfig = dagConfigs.getOrElse(
            dagRef,
            throw new Exception(
              s"Could not find dag config $dagRef referenced in ${domain.name}.${table.name}. Dag config founds ${dagConfigs.keys
                  .mkString(",")}"
            )
          )
          TableWithDagConfig(domain, table, dagRef, dagConfig, schedule)
        }
      }
    }
    tableWithDagConfigAndSchedule
  }

  case class TaskWithDagConfig(
    taskDesc: AutoTaskDesc,
    dagConfigName: String,
    dagConfig: DagGenerationConfig,
    schedule: Option[String]
  )

  private def taskWithDagConfigs(
    dagConfigs: Map[String, DagGenerationConfig]
  )(implicit settings: Settings): List[TaskWithDagConfig] = {
    logger.info("Starting to task generate dags")
    val tasks = schemaHandler.tasks()
    val taskWithDagConfigAndSchedule: List[TaskWithDagConfig] = tasks.flatMap { taskWithDag =>
      val dagConfigRef = taskWithDag.dagRef
        .orElse(settings.appConfig.dagRef.flatMap(_.transform))
      val schedule = taskWithDag.schedule
      dagConfigRef.map { dagRef =>
        val dagConfig = dagConfigs.getOrElse(
          dagRef,
          throw new Exception(
            s"Could not find dag config $dagRef referenced in ${taskWithDag.name}. Dag config founds ${dagConfigs.keys
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

  private def generateTaskDags(
    config: DagGenerateConfig
  )(implicit settings: Settings): Unit = {
    val outputDir = new Path(
      config.outputDir.getOrElse(DatasetArea.dags.toString + "/generated/transform/")
    )

    if (config.clean) {
      logger.info(s"Cleaning output directory $outputDir")
      settings.storageHandler().delete(outputDir)
    }

    val dagConfigs = schemaHandler.loadDagGenerationConfigs()
    val taskConfigs = taskWithDagConfigs(dagConfigs)
    val env = schemaHandler.activeEnvVars()
    val jEnv = env.map { case (k, v) =>
      DagPair(k, v)
    }.toList
    val depsEngine = new AutoTaskDependencies(settings, schemaHandler, settings.storageHandler())
    taskConfigs.foreach { taskConfig =>
      val dagTemplateName = taskConfig.dagConfig.template
      val dagTemplateContent = Yml2DagTemplateLoader.loadTemplate(dagTemplateName)
      val cron = settings.appConfig.schedulePresets.getOrElse(
        taskConfig.schedule.getOrElse("None"),
        taskConfig.schedule.getOrElse("None")
      )
      val cronIfNone = if (cron == "None") null else cron
      val config = AutoTaskDependenciesConfig(tasks = Some(List(taskConfig.taskDesc.name)))
      val deps = depsEngine.jobsDependencyTree(config)
      val filename = Utils.parseJinja(
        taskConfig.dagConfig.filename,
        schemaHandler.activeEnvVars() ++ Map(
          "table"  -> taskConfig.taskDesc.table,
          "domain" -> taskConfig.taskDesc.domain,
          "name"   -> taskConfig.taskDesc.name
        )
      )
      val context = TransformDagGenerationContext(
        config = taskConfig.dagConfig,
        deps = deps,
        cron = Option(cronIfNone)
      )
      applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
    }
  }
  private def generateDomainDags(
    config: DagGenerateConfig
  )(implicit settings: Settings): Unit = {
    val outputDir = new Path(
      config.outputDir.getOrElse(DatasetArea.dags.toString + "/generated/load/")
    )

    if (config.clean) {
      logger.info(s"Cleaning output directory $outputDir")
      settings.storageHandler().delete(outputDir)
    }

    val dagConfigs = schemaHandler.loadDagGenerationConfigs()
    val tableConfigs = tableWithDagConfigs(dagConfigs)
    val groupedDags = groupByDagConfigScheduleDomain(tableConfigs)

    val env = schemaHandler.activeEnvVars()
    val jEnv = env.map { case (k, v) =>
      DagPair(k, v)
    }.toList

    groupedDags.foreach { case (dagConfigName, groupedBySchedule) =>
      val dagConfig = dagConfigs(dagConfigName)
      val dagTemplateName = dagConfig.template
      val dagTemplateContent = Yml2DagTemplateLoader.loadTemplate(dagTemplateName)
      val filenameVars = dagConfig.getfilenameVars()
      if (filenameVars.exists(List("table", "finalTable").contains)) {
        if (!filenameVars.exists(List("domain", "finalDomain").contains))
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
              val schedules =
                List(DagSchedule(schedule, cronIfNone, java.util.List.of[DagDomain](dagDomain)))
              val context = LoadDagGenerationContext(config = dagConfig, schedules)

              scheduleIndex = nextScheduleIndex
              val filename = Utils.parseJinja(
                dagConfig.filename,
                schemaHandler.activeEnvVars() ++ Map(
                  "schedule"    -> scheduleValue,
                  "domain"      -> domain.name,
                  "finalDomain" -> domain.finalName,
                  "table"       -> table.name,
                  "finalTable"  -> table.finalName
                )
              )
              applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
            }
          }
        }
      } else if (filenameVars.exists(List("domain", "finalDomain").contains)) {
        // one dag per domain
        val domains = groupedBySchedule
          .flatMap { case (schedule, groupedByDomain) =>
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
                DagSchedule(schedule, cronIfNone, java.util.List.of[DagDomain](dagDomain))
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
              val context = LoadDagGenerationContext(config = dagConfig, List(schedule))
              val filename = Utils.parseJinja(
                dagConfig.filename,
                schemaHandler.activeEnvVars() ++ Map(
                  "schedule"    -> scheduleValue,
                  "domain"      -> domain.name,
                  "finalDomain" -> domain.finalName
                )
              )
              applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
            }
          } else {
            val context = LoadDagGenerationContext(config = dagConfig, schedules)
            val filename = Utils.parseJinja(
              dagConfig.filename,
              schemaHandler.activeEnvVars() ++ Map(
                "domain"      -> domain.name,
                "finalDomain" -> domain.finalName
              )
            )
            applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
          }
        }
      } else {
        // one dag per config
        groupedDags.foreach { case (dagConfigName, groupedBySchedule) =>
          var scheduleIndex = 1
          val dagConfig = dagConfigs(dagConfigName)
          val dagTemplateName = dagConfig.template
          val dagTemplateContent = Yml2DagTemplateLoader.loadTemplate(dagTemplateName)
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
              DagSchedule(scheduleValue, cronIfNone, dagDomains.asJava)
            }
            .toList
            .sortBy(_.schedule)
          if (filenameVars.contains("schedule")) {
            dagSchedules.foreach { schedule =>
              val context = LoadDagGenerationContext(config = dagConfig, List(schedule))
              val filename = Utils.parseJinja(
                dagConfig.filename,
                schemaHandler.activeEnvVars() ++ Map(
                  "schedule" -> schedule
                )
              )
              applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
            }
          } else {
            val context = LoadDagGenerationContext(config = dagConfig, schedules = dagSchedules)
            val filename = Utils.parseJinja(
              dagConfig.filename,
              schemaHandler.activeEnvVars()
            )
            applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context.asMap, filename)
          }

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
}

object DagGenerateCommand {
  val SCHEDULE = "schedule"
  val name = "generate"
}
