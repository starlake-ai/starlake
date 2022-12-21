package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
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

  def run(config: AutoTask2GraphVizConfig): Unit = {
    jobAsDot(config)
  }

  def jobAsDot(config: AutoTask2GraphVizConfig): List[(String, String)] = {
    val tasks = AutoTask.tasks(config.reload)(settings, storageHandler, schemaHandler)
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
    depsMap.foreach { case (jobName, allDeps) =>
      val deps =
        allDeps.filter(dep => config.objects.contains("all") || config.objects.contains(dep.typ))
      val dedupEntities = deps.groupBy(_.name).mapValues(_.head).values
      val entitiesAsDot = dedupEntities.map(dep => dep.entityAsDot()).mkString("\n")
      val relationsAsDot = deps
        .filter(dep => config.objects.contains(dep.parentTyp))
        .flatMap(dep => dep.relationAsDot())
        .distinct
        .mkString("\n")
      val result = List(prefix, entitiesAsDot, relationsAsDot, suffix).mkString("\n")
      config.outputDir match {
        case Some(outputDir) =>
          val dir = File(outputDir)
          dir.createDirectoryIfNotExists(true)
          val file = File(outputDir, s"$jobName.dot")
          file.overwrite(result)
        case None =>
          println(result)
      }

    }

    val results = depsMap.map { case (jobName, allDeps) =>
      val deps =
        allDeps.filter(dep => config.objects.contains("all") || config.objects.contains(dep.typ))
      val dedupEntities = deps.groupBy(_.name).mapValues(_.head).values
      val entitiesAsDot = dedupEntities.map(dep => dep.entityAsDot()).mkString("\n")
      val relationsAsDot = deps
        .filter(dep => config.objects.contains(dep.parentTyp))
        .flatMap(dep => dep.relationAsDot())
        .distinct
        .mkString("\n")
      (jobName, List(prefix, entitiesAsDot, relationsAsDot, suffix).mkString("\n"))
    }
    config.outputDir match {
      case Some(outputDir) =>
        val dir = File(outputDir)
        dir.createDirectoryIfNotExists(true)
        results map { case (jobName, result) =>
          val file = File(outputDir, s"$jobName.dot")
          file.overwrite(result)
        }
        results
      case None =>
        results
    }

  }
}
