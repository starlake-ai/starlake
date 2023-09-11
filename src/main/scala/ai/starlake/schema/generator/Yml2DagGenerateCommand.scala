package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

class Yml2DagGenerateCommand(schemaHandler: SchemaHandler) extends LazyLogging {

  def run(args: Array[String])(implicit settings: Settings): Unit = {
    Yml2DagGenerateConfig.parse(args) match {
      case Some(config) =>
        generateDomainDags(config)
      case _ =>
        println(Yml2DagGenerateConfig.usage())
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
        val dagRef = mergedMetadata.dagRef
          .orElse(settings.appConfig.dagRef)
        val schedule = mergedMetadata.schedule

        dagRef.map { dagRef =>
          val dagConfig = dagConfigs.getOrElse(
            dagRef,
            throw new Exception(
              s"Could not find dag config $dagRef referenced in ${domain.name}.${table.name}"
            )
          )
          TableWithDagConfig(domain, table, dagRef, dagConfig, schedule)
        }
      }
    }
    tableWithDagConfigAndSchedule
  }
  private def generateDomainDags(
    config: Yml2DagGenerateConfig
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
      if (filenameVars.contains("table")) {
        if (!filenameVars.contains("domain"))
          logger.warn(
            s"Dag Config $dagConfigName: filename contains table but not domain, this will generate multiple dags with the same name if the same table name appear in multiple domains"
          )
        // one dag per table
        groupedBySchedule.foreach { case (schedule, groupedByDomain) =>
          groupedByDomain.foreach { case (domainName, tableNames) =>
            tableNames.foreach { tableName =>
              val dagDomain = DagDomain(domainName, java.util.List.of[String](tableName))
              val schedules = List(DagSchedule(schedule, java.util.List.of[DagDomain](dagDomain)))
              val context = DagGenerationContext(config = dagConfig, schedules)
              val filename = Utils.parseJinja(
                dagConfig.filename,
                schemaHandler.activeEnvVars() ++ Map(
                  "domain" -> domainName,
                  "table"  -> tableName
                )
              )
              applyJ2AndSave(outputDir, jEnv, dagTemplateContent, context, filename)
            }
          }
        }
      } else if (filenameVars.contains("domain")) {
        // one dag per domain
        val domainNames = groupedBySchedule.flatMap { case (schedule, groupedByDomain) =>
          groupedByDomain.map { case (domainName, tableNames) =>
            domainName
          }
        }
        domainNames.foreach { domainName =>
          val schedules = groupedBySchedule.map { case (schedule, groupedByDomain) =>
            val tables = groupedByDomain(domainName)
            val dagDomain = DagDomain(domainName, tables.asJava)
            DagSchedule(schedule, java.util.List.of[DagDomain](dagDomain))
          }.toList

          val context = DagGenerationContext(config = dagConfig, schedules)
          val filename = Utils.parseJinja(
            dagConfig.filename,
            schemaHandler.activeEnvVars() ++ Map("domain" -> domainName)
          )
          applyJ2AndSave(DatasetArea.dags, jEnv, dagTemplateContent, context, filename)
        }
      } else {
        // one dag per config
        groupedDags.foreach { case (dagConfigName, groupedBySchedule) =>
          val dagConfig = dagConfigs(dagConfigName)
          val dagTemplateName = dagConfig.template
          val dagTemplateContent = Yml2DagTemplateLoader.loadTemplate(dagTemplateName)
          val dagSchedules = groupedBySchedule.map { case (schedule, groupedByDomain) =>
            val dagDomains = groupedByDomain.map { case (domainName, tableNames) =>
              DagDomain(domainName, tableNames.asJava)
            }.toList
            DagSchedule(schedule, dagDomains.asJava)
          }.toList

          val context = DagGenerationContext(config = dagConfig, schedules = dagSchedules)
          val filename = Utils.parseJinja(
            dagConfig.filename,
            schemaHandler.activeEnvVars()
          )
          applyJ2AndSave(DatasetArea.dags, jEnv, dagTemplateContent, context, filename)
        }
      }
    }
  }

  private def applyJ2AndSave(
    outputDir: Path,
    jEnv: List[DagPair],
    dagTemplateContent: String,
    context: DagGenerationContext,
    filename: String
  )(implicit settings: Settings): Unit = {
    val jContext = context.asMap
    val paramMap = Map(
      "context" -> jContext,
      "env"     -> jEnv
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
  ): Map[String, Map[String, Map[String, List[String]]]] = {
    val groupByDagConfigName = tableWithDagConfigs.groupBy(_.dagConfigName)
    val groupDagConfigNameAndSchedule = groupByDagConfigName.map {
      case (dagConfigName, tableWithDagConfigs) =>
        val groupedBySchedule = tableWithDagConfigs.groupBy(_.schedule.getOrElse("None"))
        val groupedByScheduleAndDomain = groupedBySchedule.map {
          case (scheduleName, tableWithDagConfigs) =>
            val groupedByDomain = tableWithDagConfigs.groupBy(_.domain.name)
            val groupedTableNames = groupedByDomain.map { case (domain, tableWithConfig) =>
              domain -> tableWithConfig.map(_.table.name)
            }
            scheduleName -> groupedTableNames
        }
        (dagConfigName, groupedByScheduleAndDomain)
    }
    groupDagConfigNameAndSchedule
  }
}

object Yml2DagGenerateCommand {
  val SCHEDULE = "schedule"
  val name = "generate"
}
