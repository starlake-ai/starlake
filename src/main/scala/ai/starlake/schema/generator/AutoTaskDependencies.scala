package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.StrictLogging

import scala.util.Try

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

  def run(config: AutoTaskDependenciesConfig): Try[Unit] = Try {
    val allDependencies: List[DependencyContext] = tasks(config)
    if (config.print) jobsDependencyTree(allDependencies, config)
    if (config.viz) jobAsDot(allDependencies, config)
  }

  /** @param config
    * @return
    *   List[DependencyContext(jobName, dedupEntities, relations)]
    */
  def tasks(
    config: AutoTaskDependenciesConfig
  ): List[DependencyContext] = {
    val tasks =
      AutoTask.unauthenticatedTasks(config.reload)(settings, storageHandler, schemaHandler)
    val depsMap = {
      /*
      if (config.verbose) {
        schemaHandler
          .tasks()
          .map { task =>
            (task.name, TaskViewDependency.taskDependencies(task.name, tasks)(schemaHandler))
          } :+ ("_lineage" -> TaskViewDependency.dependencies(tasks)(schemaHandler))
      } else {
      }

       */
      config.tasks.getOrElse(Nil) match {
        case Nil =>
          if (config.all)
            List("_lineage" -> TaskViewDependency.dependencies(tasks)(schemaHandler))
          else
            Nil
        case taskOrDomainNames =>
          val taskNames =
            if (taskOrDomainNames.size == 1) {
              val taskOrDomainName = taskOrDomainNames.head
              if (taskOrDomainName.contains('.'))
                taskOrDomainNames
              else {
                tasks.filter(_.taskDesc.domain == taskOrDomainName).map(_.taskDesc.name)
              }
            } else {
              taskOrDomainNames
            }
          taskNames.map(taskName =>
            (taskName, TaskViewDependency.taskDependencies(taskName, tasks)(schemaHandler))
          )
      }
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
    }.toList
  }

  def jobsDependencyTree(
    config: AutoTaskDependenciesConfig
  ): List[TaskViewDependencyNode] = {
    jobsDependencyTree(tasks(config), config)
  }

  def jobsDependencyTree(
    allDependencies: List[DependencyContext],
    config: AutoTaskDependenciesConfig
  ): List[TaskViewDependencyNode] = {

    val relations = allDependencies.flatMap(_.relations)
    val entities = allDependencies.flatMap(_.entities)
    val result = config.tasks match {
      case Some(taskNames) =>
        val taskEntities = taskNames.map { taskName =>
          val taskEntity = entities.find(_.name == taskName).getOrElse {
            throw new RuntimeException(s"taskName:$taskName not found")
          }
          taskEntity
        }
        taskEntities.map(entity => TaskViewDependencyNode.dependencies(entity, entities, relations))
      case None =>
        if (config.all)
          TaskViewDependencyNode.dependencies(entities, relations)
        else
          Nil

    }
    result.foreach(_.print())
    result.toList
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

  def jobAsDot(config: AutoTaskDependenciesConfig): Unit = {
    jobAsDot(tasks(config), config)
  }

  /** @param allDependencies
    * @param config
    * @return
    *   (jobName, dotContent)
    */
  def jobAsDot(
    allDependencies: List[DependencyContext],
    config: AutoTaskDependenciesConfig
  ): Unit = {
    def distinctBy[A, B](xs: List[A])(f: A => B): List[A] =
      scala.reflect.internal.util.Collections.distinctBy(xs)(f)

    val dedupDependencies = allDependencies.foldLeft(DependencyContext("all", Nil, Nil)) {
      (acc, dep) =>
        DependencyContext(
          dep.jobName,
          distinctBy(acc.entities ++ dep.entities)(_.name),
          acc.relations ++ dep.relations
        )
    }

    val entitiesAsDot = dedupDependencies.entities.map(dep => dep.entityAsDot()).mkString("\n")
    val relationsAsDot = dedupDependencies.relations
      .flatMap(dep => dep.relationAsDot())
      .distinct
      .mkString("\n")
    val dotContent = List(prefix, entitiesAsDot, relationsAsDot, suffix).mkString("\n")

    if (config.svg)
      Utils.dot2Svg(config.outputFile, dotContent)
    else if (config.png)
      Utils.dot2Png(config.outputFile, dotContent)
    else
      Utils.save(config.outputFile, dotContent)
  }
}
