package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.utils.Utils
import better.files.File
import com.typesafe.scalalogging.StrictLogging

case class DependencyContext(
  jobName: String,
  entities: List[TaskViewDependency],
  relations: List[TaskViewDependency]
)
class AutoTaskDependencies(
  settings: Settings,
  schemaHandler: SchemaHandler,
  storageHandler: StorageHandler
) extends StrictLogging {

  def run(config: AutoTaskDependenciesConfig): Unit = {
    val allDependencies: List[DependencyContext] = jobs(config)
    if (config.print) jobsDependencyTree(allDependencies, config)
    if (config.viz) jobAsDot(allDependencies, config)
  }

  /** @param config
    * @return
    *   List[DependencyContext(jobName, dedupEntities, relations)]
    */
  def jobs(
    config: AutoTaskDependenciesConfig
  ): List[DependencyContext] = {
    val tasks =
      AutoTask.unauthenticatedTasks(config.reload)(settings, storageHandler, schemaHandler)
    val depsMap =
      if (config.verbose) {
        schemaHandler
          .tasks()
          .map { task =>
            (task.name, TaskViewDependency.taskDependencies(task.name, tasks)(schemaHandler))
          } :+ ("_lineage" -> TaskViewDependency.dependencies(tasks)(schemaHandler))
      } else {
        val (taskName, deps) = config.task
          .map(taskName =>
            (taskName, TaskViewDependency.taskDependencies(taskName, tasks)(schemaHandler))
          )
          .getOrElse("_lineage" -> TaskViewDependency.dependencies(tasks)(schemaHandler))
        List(taskName -> deps)
      }
    val mapper = Utils.newJsonMapper().writerWithDefaultPrettyPrinter()

    depsMap.map { case (jobName, allDeps) =>
      val deps =
        allDeps.filter(dep => config.objects.contains("all") || config.objects.contains(dep.typ))
      val dedupEntities = deps.groupBy(_.name).mapValues(_.head).values.toList
      val relations = deps
        .filter(dep => config.objects.contains(dep.parentTyp))

      logger.whenDebugEnabled {
        logger.debug(s"----------jobName:$jobName")
        logger.debug("----------relations------")
        logger.debug(mapper.writeValueAsString(relations))
      }
      DependencyContext(jobName, dedupEntities, relations)
    }
  }

  def jobsDependencyTree(
    config: AutoTaskDependenciesConfig
  ): List[TaskViewDependencyNode] = {
    jobsDependencyTree(jobs(config), config)
  }

  def jobsDependencyTree(
    allDependencies: List[DependencyContext],
    config: AutoTaskDependenciesConfig
  ): List[TaskViewDependencyNode] = {

    val relations = allDependencies.flatMap(_.relations)
    val entities = allDependencies.flatMap(_.entities)
    val result = config.task match {
      case Some(taskName) =>
        val entity = entities.find(_.name == taskName).getOrElse {
          throw new RuntimeException(s"taskName:$taskName not found")
        }
        List(TaskViewDependencyNode.dependencies(entity, entities, relations))

      case None =>
        TaskViewDependencyNode.dependencies(entities, relations)
    }
    result.foreach(_.print())
    result
  }

  //// DOT section
  val prefix =
    """
      |digraph {
      |graph [pad="0.5", nodesep="0.5", ranksep="2"];
      |node [shape=plain]
      |rankdir=LR;
      |
      |
      |""".stripMargin

  val aclPrefix =
    """
      |digraph {
      |graph [pad="0.5", nodesep="0.5", ranksep="2"];
      |
      |
      |""".stripMargin

  val suffix =
    """
      |}
      |""".stripMargin

  def jobAsDot(config: AutoTaskDependenciesConfig): List[(String, String)] =
    jobAsDot(jobs(config), config)

  def jobAsDot(
    allDependencies: List[DependencyContext],
    config: AutoTaskDependenciesConfig
  ): List[(String, String)] = {
    val results: List[(String, String)] = allDependencies.map {
      case DependencyContext(jobName, dedupEntities, relations) =>
        val entitiesAsDot = dedupEntities.map(dep => dep.entityAsDot()).mkString("\n")
        val relationsAsDot = relations
          .flatMap(dep => dep.relationAsDot())
          .distinct
          .mkString("\n")
        (jobName, List(prefix, entitiesAsDot, relationsAsDot, suffix).mkString("\n"))
    }
    config.outputDir match {
      case Some(outputDir) =>
        val dir = File(outputDir)
        dir.createDirectoryIfNotExists(createParents = true)
        results map { case (jobName, result) =>
          val file = File(outputDir, s"$jobName.dot")
          file.overwrite(result)
        }
        results
      case None =>
        results.foreach { case (jobName, result) =>
          println(result)
        }
        results
    }
  }
}
