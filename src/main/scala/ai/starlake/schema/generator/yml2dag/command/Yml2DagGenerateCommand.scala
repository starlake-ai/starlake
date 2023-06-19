package ai.starlake.schema.generator.yml2dag.command

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import ai.starlake.schema.generator.yml2dag.config.Yml2DagGenerateConfig
import ai.starlake.schema.generator.yml2dag.{DomainTemplate, Yml2DagTemplateLoader}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class Yml2DagGenerateCommand(schemaHandler: SchemaHandler) extends LazyLogging {

  def run(yml2DagGenerateConfig: Yml2DagGenerateConfig): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    yml2DagGenerateConfig.domainTemplatePath match {
      case Some(domainTemplatePath) =>
        generateDomainDags(domainTemplatePath) match {
          case Failure(exception) => logger.error(exception.getMessage, exception)
          case Success(_)         => logger.info("Sucessfully generated dags")
        }
      case None => // Do nothing
    }
  }

  private def generateDomainDags(
    domainTemplatePath: String
  )(implicit settings: Settings): Try[Unit] = {
    logger.info("Starting to generate dags")
    for {
      dagTemplate <- Yml2DagTemplateLoader.loadTemplate(DomainTemplate(domainTemplatePath))
      defaultDagGenerationConfig <- schemaHandler.loadDagGenerationConfig(DatasetArea.dags)
      propagatedMetadataDomains = schemaHandler
        .domains()
        .map(propagateMetadata(defaultDagGenerationConfig, _))
      _ <- assertDagGenerationConfigRequirements(propagatedMetadataDomains)
      dagGroups = groupByDagName(propagatedMetadataDomains)
      domainWithGroupedTables = groupTables(dagGroups)
      _ <- assertDagGenerationGroupRequirements(domainWithGroupedTables)
    } yield {
      domainWithGroupedTables.foreach { case (dagName, dagGroups) =>
        dagGroups.zipWithIndex.map { case (domainTables, i) =>
          // we've previously grouped by dag generation config so getting the first one for all generation is correct.
          val dagGenerationConfig = resolveGenerationConfig(domainTables)
          val effectiveDagName = dagGenerationConfig.suffixWithIndex match {
            case Some(true) | None => s"$dagName-$i"
            case _                 => dagName
          }
          logDagGenerationStart(domainTables, effectiveDagName)
          val dag = Utils.parseJinjaTpl(
            dagTemplate,
            Map(
              "sl" -> DagGenerationContext(
                config = dagGenerationConfig,
                domainTables = domainTables.asJava
              ),
              "env" -> schemaHandler.activeEnvVars().asJava
            )
          )
          val outputPath =
            Path.mergePaths(DatasetArea.dags, new Path(s"/generated/domains/$effectiveDagName.py"))
          logger.info(s"Writing dag to $outputPath")
          settings.storageHandler.write(dag, outputPath)(StandardCharsets.UTF_8)
        }
      }
      logger.info("Successfully generated dags")
      println("Successfully generated dags")
    }
  }

  private def logDagGenerationStart(
    domainTables: List[DomainTable],
    effectiveDagName: String
  ): Unit = {
    val tables = domainTables
      .map { case DomainTable(domain, table) => s"${domain.name}.${table.name}" }
      .mkString("\n\t - ", "\n\t - ", "")
    logger.info(s"Generating dag $effectiveDagName with the following tables: $tables")
  }

  private def resolveGenerationConfig(domainTables: List[DomainTable]) = {
    domainTables
      .map(_.table)
      .headOption
      .flatMap(_.metadata)
      .flatMap(_.dag)
      .getOrElse(
        throw new RuntimeException(
          "Doesn't have any dag generation config. Should never happen."
        )
      )
  }

  case class DagGenerationContext(
    config: DagGenerationConfig,
    domainTables: java.util.List[DomainTable]
  )

  case class DomainTable(domain: Domain, table: Schema)
  private def propagateMetadata(
    defaultDagGenerationConfig: DagGenerationConfig,
    domain: Domain
  ): Domain = {
    val domainMetadata = domain.metadata.getOrElse(Metadata())
    val mergedDomainDagGenerationConfig = domainMetadata.dag
      .map(DagGenerationConfig.dagGenerationConfigMerger(defaultDagGenerationConfig, _))
      .getOrElse(defaultDagGenerationConfig)
    val mergedDomainMetadata = domainMetadata.copy(dag = Some(mergedDomainDagGenerationConfig))
    val tables = domain.tables.map(t => {
      val mergedTableMetadata = mergedDomainMetadata.merge(t.metadata.getOrElse(Metadata()))
      t.copy(metadata = Some(mergedTableMetadata))
    })
    domain.copy(tables = tables)
  }

  /** @param domains
    * @return
    *   a map containing a dag name and the list of tables along with their domain that are grouped
    *   together.
    */
  private def groupByDagName(
    domains: List[Domain]
  )(implicit settings: Settings): Map[String, List[DomainTable]] = {
    domains.flatMap(d => d.tables.map(DomainTable(d, _))).groupBy { case DomainTable(d, t) =>
      val dagNamePattern = t.metadata
        .flatMap(_.dag)
        .flatMap(_.dagName)
        .getOrElse(DagGenerationConfig.defaultDagNamePattern)
      val datasetId = BigQueryJobBase.extractProjectDataset(d.name)
      formatDagName(
        dagNamePattern,
        Option(datasetId.getProject),
        datasetId.getDataset,
        t.name
      )
    }
  }

  private def groupTables(
    dagGroups: Map[String, List[DomainTable]]
  ): Map[String, List[List[DomainTable]]] = {
    dagGroups.mapValues(dagElements => {
      dagElements
        .groupBy(_.table.metadata.flatMap(_.dag))
        .values
        .toList
        .map(_.sortBy(dt => dt.domain.name + "." + dt.table.name))
        .sortBy(
          _.size
        ) // sort by size for the moment but need to propagate index to domain file in order to make output sticky.
    })
  }

  /** reject tables if one the following criteria is met :
    *   - metadata is empty (should not happen because of initialisation)
    *   - dag generation config is empty
    *   - dataprocProfileVar or slRootVar is not defined
    *
    * @param domains
    * @return
    */
  private def assertDagGenerationConfigRequirements(domains: List[Domain]): Try[Unit] = {
    val tablesMissingRequirements = domains
      .flatMap(d => d.tables.map(d.name -> _))
      .filter { case (_, table) =>
        !table.metadata
          .flatMap(_.dag)
          .exists(conf =>
            conf.cluster.isDefined && conf.cluster.exists(cluster =>
              cluster.region.isDefined && cluster.profileVar.isDefined
            ) && conf.sl.isDefined && conf.sl
              .exists(slc => slc.envVar.isDefined && slc.jarFileUrisVar.isDefined)
          )
      }
    if (tablesMissingRequirements.isEmpty) {
      Success(())
    } else {
      val tableList = tablesMissingRequirements
        .map { case (domainName, table) => s"$domainName.${table.name}" }
        .mkString(", ")
      Failure(
        new RuntimeException(
          "Tables did not define cluster.profileVar, cluster.region, sl.envVar or sl.jarFileUrisVar. Please set this on global dag generation config in one of the following location: dags/default.comet.yml, domain/metadata/dags, table/metadata/dags.\n" + tableList
        )
      )
    }
  }

  /** reject grouping if suffixWithIndex is false for more than one element in the group because
    * generation will have some overlap
    *
    * @param dagAndTables
    * @return
    */
  private def assertDagGenerationGroupRequirements(
    dagAndTables: Map[String, List[List[DomainTable]]]
  ): Try[Unit] = {
    def extractSuffixWithIndex(group: List[DomainTable]): Boolean = {
      group.headOption
        .flatMap(_.table.metadata.flatMap(_.dag).flatMap(_.suffixWithIndex))
        .getOrElse(DagGenerationConfig.defaultSuffixWithIndex)
    }
    val outputConflict = dagAndTables.filter { case (_, dagOutputs) =>
      dagOutputs.size > 1 && // keep only dag name with more than one output
      dagOutputs.count(extractSuffixWithIndex(_) == false) > 1
    }
    dagAndTables.flatMap { case (_, dagOutputs) => dagOutputs }.flatten.foreach {
      case DomainTable(domain, table) =>
        logger.info(
          s"${domain.name}.${table.name}:" + table.metadata
            .flatMap(_.dag)
            .flatMap(_.suffixWithIndex)
            .getOrElse(false)
        )
    }
    val conflictMessages = outputConflict.map { case (dagName, dagOutputs) =>
      val tableList = dagOutputs.zipWithIndex
        .map { case (group, groupIndex) =>
          val suffixWithIndex = extractSuffixWithIndex(group)
          val tables = group
            .map { case DomainTable(domain, schema) => s"${domain.name}.${schema.name}" }
            .mkString(", ")
          s"dag group $groupIndex (suffix with index: $suffixWithIndex) : $tables"
        }
        .mkString("\n\t -", "\n\t -", "")
      s"""$dagName need to generate ${dagOutputs.length} but more than one group disabled dag name suffix with index
        making output conflict.$tableList""".stripMargin.strip()
    }
    if (conflictMessages.isEmpty) {
      Success(())
    } else {
      Failure(
        new RuntimeException(
          conflictMessages.mkString("\n")
        )
      )
    }
  }

  private def formatDagName(
    dagNamePattern: String,
    bqProjectId: Option[String],
    domain: String,
    table: String
  )(implicit settings: Settings): String = {
    Utils.parseJinjaTpl(
      dagNamePattern,
      Map(
        "bq_project_id" -> bqProjectId.getOrElse(""),
        "domain"        -> domain,
        "table"         -> table
      )
    )
  }
}

object Yml2DagGenerateCommand {
  val SCHEDULE = "schedule"
  val name = "generate"
}
