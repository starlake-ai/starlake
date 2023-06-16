package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.utils.Utils
import better.files.File
import com.typesafe.scalalogging.StrictLogging

class AutoTaskToGraphViz(
  settings: Settings,
  schemaHandler: SchemaHandler,
  storageHandler: StorageHandler
) extends StrictLogging {

  val prefix = """
                 |digraph {
                 |graph [pad="0.5", nodesep="0.5", ranksep="2"];
                 |node [shape=plain]
                 |rankdir=LR;
                 |
                 |
                 |""".stripMargin

  val aclPrefix = """
                    |digraph {
                    |graph [pad="0.5", nodesep="0.5", ranksep="2"];
                    |
                    |
                    |""".stripMargin

  val suffix = """
                 |}
                 |""".stripMargin

  def run(config: AutoTask2GraphVizConfig): Unit = jobAsDot(config)

  def jobs(
    config: AutoTask2GraphVizConfig
  ): List[(String, List[TaskViewDependency], List[TaskViewDependency])] = {
    val tasks =
      AutoTask.unauthenticatedTasks(config.reload)(settings, storageHandler, schemaHandler)
    val depsMap =
      if (config.verbose) {
        schemaHandler
          .jobs()
          .keys
          .map { jobName =>
            (jobName, TaskViewDependency.jobDependencies(jobName, tasks)(schemaHandler))
          }
          .toList :+ ("_lineage" -> TaskViewDependency.dependencies(tasks)(schemaHandler))
      } else {
        val (jobName, deps) = config.job
          .map(jobName =>
            (jobName, TaskViewDependency.jobDependencies(jobName, tasks)(schemaHandler))
          )
          .getOrElse("_lineage" -> TaskViewDependency.dependencies(tasks)(schemaHandler))
        List(jobName -> deps)
      }
    val mapper = Utils.newJsonMapper().writerWithDefaultPrettyPrinter()

    depsMap.map { case (jobName, allDeps) =>
      val deps =
        allDeps.filter(dep => config.objects.contains("all") || config.objects.contains(dep.typ))
      val dedupEntities = deps.groupBy(_.name).mapValues(_.head).values.toList
      val relations = deps
        .filter(dep => config.objects.contains(dep.parentTyp))

      logger.debug(s"----------jobName:$jobName")
      logger.debug("----------relations------")
      logger.debug(mapper.writeValueAsString(System.out, relations))
      (jobName, dedupEntities, relations)
    }
  }

  def jobAsDot(config: AutoTask2GraphVizConfig): List[(String, String)] = {
    val results: List[(String, String)] = jobs(config).map {
      case (jobName, dedupEntities, relations) =>
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

  def oldJobAsDot(config: AutoTask2GraphVizConfig): List[(String, String)] = {
    val tasks =
      AutoTask.unauthenticatedTasks(config.reload)(settings, storageHandler, schemaHandler)
    val depsMap =
      if (config.verbose) {
        schemaHandler
          .jobs()
          .keys
          .map { jobName =>
            (jobName, TaskViewDependency.jobDependencies(jobName, tasks)(schemaHandler))
          }
          .toList :+ ("_lineage" -> TaskViewDependency.dependencies(tasks)(schemaHandler))
      } else {
        val (jobName, deps) = config.job
          .map(jobName =>
            (jobName, TaskViewDependency.jobDependencies(jobName, tasks)(schemaHandler))
          )
          .getOrElse("_lineage" -> TaskViewDependency.dependencies(tasks)(schemaHandler))
        List(jobName -> deps)
      }
    val mapper = Utils.newJsonMapper().writerWithDefaultPrettyPrinter()

    val results = depsMap.map { case (jobName, allDeps) =>
      val deps =
        allDeps.filter(dep => config.objects.contains("all") || config.objects.contains(dep.typ))
      val dedupEntities = deps.groupBy(_.name).mapValues(_.head).values.toList
      val relations = deps
        .filter(dep => config.objects.contains(dep.parentTyp))
      println(s"----------jobName:$jobName")
      println("----------relations------")
      mapper.writeValue(System.out, relations)
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
