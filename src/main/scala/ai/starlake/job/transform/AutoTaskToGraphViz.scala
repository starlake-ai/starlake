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
    val tasks = AutoTask.tasks(config.reload)(settings, storageHandler, schemaHandler)
    val deps = config.job
      .map(TaskViewDependency.jobDependencies(_, tasks)(schemaHandler))
      .getOrElse(TaskViewDependency.dependencies(tasks)(schemaHandler))
    val dedup = deps.groupBy(_.name).mapValues(_.head).values
    val entitiesAsDot = dedup.map(dep => dep.entityAsDot()).mkString("\n")
    val relationsAsDot = deps.flatMap(dep => dep.relationAsDot()).mkString("\n")
    val result = List(prefix, entitiesAsDot, relationsAsDot, suffix).mkString("\n")
    config.output match {
      case Some(output) =>
        val file = File(output)
        file.parent.createDirectoryIfNotExists(true)
        file.overwrite(result)
      case None =>
        println(result)
    }
  }
}
