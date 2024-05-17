package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.generator
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Domain, Schema}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/*
https://medium.com/hibob-engineering/from-list-to-immutable-hierarchy-tree-with-scala-c9e16a63cb89
 */

object TaskViewDependency extends StrictLogging {
  // TODO migrate to enum once scala 3 is here
  val TASK_TYPE: String = "task"
  val CTE_TYPE: String = "cte"
  val TASKVIEW_TYPE: String = "taskview"
  val TABLE_TYPE: String = "table"
  val VIEW_TYPE: String = "view"
  val UNKNOWN_TYPE: String = "unknown"

  def typLabel(typ: String): String = typ match {
    case TASK_TYPE     => "Transform"
    case CTE_TYPE      => "CTE"
    case TASKVIEW_TYPE => "Taskview"
    case TABLE_TYPE    => "Table"
    case VIEW_TYPE     => "View"
    case _             => "Unknown"
  }
  case class SimpleEntry(name: String, typ: String, parentRefs: List[String])
  def taskDependencies(taskName: String, tasks: List[AutoTask])(implicit
    settings: Settings,
    schemaHandler: SchemaHandler
  ): List[TaskViewDependency] = {
    val deps = dependencies(tasks)
    var result = scala.collection.mutable.ListBuffer[TaskViewDependency]()
    val roots = deps.filter(t => t.name == taskName && t.typ == TASK_TYPE)
    result ++= roots
    getHierarchy(roots, deps, result)
    result.toList.distinct
  }

  private def getHierarchy(
    roots: List[TaskViewDependency],
    allDeps: List[TaskViewDependency],
    result: ListBuffer[TaskViewDependency]
  ): Unit = {
    roots.foreach { root =>
      val subRoots = allDeps.filter(t =>
        t.typ == root.parentTyp && t.name.toLowerCase() == root.parent.toLowerCase()
      )
      val nocyclicRoots = subRoots.filter { subRoot =>
        val cycle = roots.exists(_.name.toLowerCase() == subRoot.name.toLowerCase())
        !cycle
      }
      result ++= nocyclicRoots
      getHierarchy(nocyclicRoots, allDeps, result)
    }
  }

  def dependencies(
    tasks: List[AutoTask]
  )(implicit settings: Settings, schemaHandler: SchemaHandler): List[TaskViewDependency] = {
    val jobs: Map[String, List[AutoTask]] = tasks.groupBy(_.name)
    val jobDependencies: List[SimpleEntry] =
      jobs.mapValues(_.flatMap(_.dependencies())).toList.map { case (jobName, dependencies) =>
        SimpleEntry(jobName, TASK_TYPE, dependencies)
      }

    val jobAndViewDeps = jobDependencies.flatMap { case SimpleEntry(jobName, typ, parentRefs) =>
      logger.info(
        s"Analyzing dependency of type '$typ' for job '$jobName' with parent refs [${parentRefs.mkString(",")}]"
      )
      if (parentRefs.isEmpty)
        List(TaskViewDependency(jobName, typ, "", UNKNOWN_TYPE, ""))
      else {
        parentRefs.map { parentSQLRef =>
          val parts = parentSQLRef.split('.')
          // is it a job ?
          val parentJobName = parts.length match {
            case 1 =>
              val tablePart = parts.last // == 0
              val refs =
                tasks.filter(task =>
                  task.taskDesc.name.endsWith(s".$tablePart") ||
                  task.taskDesc.table.toLowerCase() == tablePart.toLowerCase()
                )
              if (refs.size > 1) {
                throw new Exception(
                  s"""invalid parent ref '$parentSQLRef' syntax in job '$jobName': Too many tasks found ${refs
                      .map(_.name)
                      .mkString(",")}.
                     |Make sure references in your SQL are unambiguous or fully qualified.
                     |""".stripMargin
                )

              } else
                refs.headOption.map(ref => (ref.name, ref.taskDesc.schedule))

            case 2 | 3 =>
              val domainPart = parts.dropRight(1).last
              val tablePart = parts.last
              tasks
                .find(task =>
                  task.taskDesc.name.toLowerCase() == s"$domainPart.$tablePart".toLowerCase() ||
                  (task.taskDesc.table.toLowerCase() == tablePart.toLowerCase() &&
                  task.taskDesc.domain.toLowerCase() == domainPart.toLowerCase())
                )
                .map(ref => (ref.name, ref.taskDesc.schedule))
            case _ =>
              val errors = schemaHandler.checkJobsVars().mkString("\n")

              // Strange. This should never happen as far as I know. Let's make it clear
              if (parts.length == 0)
                throw new Exception(
                  s"""invalid parent ref '$parentSQLRef' syntax in job '$jobName': No part found.
                     |Make sure variables defined in your job have a default value in the selected env profile.
                     |$errors""".stripMargin
                )
              else
                // Strange. This should never happen as far as I know. Let's make it clear
                throw new Exception(
                  s"""invalid parent ref '$parentSQLRef' syntax in job '$jobName'. Too many parts.
                       |Make sure variables defined in your job have a default value in the selected env profile.
                       |$errors""".stripMargin
                )

          }
          parentJobName match {
            case Some(value) =>
              val result =
                generator.TaskViewDependency(
                  jobName,
                  typ,
                  value._1,
                  TASK_TYPE,
                  parentSQLRef,
                  None,
                  Some(
                    settings.appConfig.schedulePresets.getOrElse(
                      value._2.getOrElse("None"),
                      value._2.getOrElse("None")
                    )
                  )
                )
              if (typ == TASK_TYPE) {
                // TODO We just handle one task per job which is always the case till now.
                val task = jobs(jobName).head
                val sink = task.taskDesc.domain + "." + task.taskDesc.table
                result.copy(sink = Some(sink))
              } else result
            case None =>
              // is it a table ?
              val domains = schemaHandler.domains()
              val parentTable = parts.length match {
                case 1 =>
                  val tablePart = parts.last // == 0
                  @tailrec
                  def dt(ds: List[Domain]): Option[(Domain, Schema)] = {
                    ds match {
                      case x :: xs =>
                        x.tables.find(
                          _.finalName.toLowerCase() == tablePart.toLowerCase()
                        ) match {
                          case Some(table) => Some(x, table)
                          case _           => dt(xs)
                        }
                      case Nil => None
                    }
                  }
                  val domainWithTable: Option[(Domain, Schema)] = dt(domains)
                  val schedule = domainWithTable.flatMap(_._2.metadata).flatMap(_.schedule)
                  domainWithTable.map(dt => (dt._1.finalName, tablePart, schedule))

                case 2 | 3 =>
                  val domainPart = parts.dropRight(1).last
                  val tablePart = parts.last
                  val theDomain: Option[Domain] = domains
                    .find(_.finalName.toLowerCase() == domainPart.toLowerCase())
                  val theTable = theDomain.flatMap(
                    _.tables.find(table => table.finalName.toLowerCase() == tablePart.toLowerCase())
                  )
                  val schedule = theTable.flatMap(_.metadata).flatMap(_.schedule)
                  val parentDomainFound = theTable.nonEmpty
                  if (parentDomainFound)
                    Some((domainPart, tablePart, schedule))
                  else
                    None
                case _ =>
                  // Strange. This should never happen as far as I know. Let's log it
                  throw new Exception(s"unknown $parentSQLRef syntax. Too many parts")
              }
              parentTable match {
                case Some((parentDomainName, parentTableName, schedule)) =>
                  TaskViewDependency(
                    jobName,
                    typ,
                    parentDomainName + "." + parentTableName,
                    TABLE_TYPE,
                    parentSQLRef,
                    None,
                    Some(
                      settings.appConfig.schedulePresets.getOrElse(
                        schedule.getOrElse("None"),
                        schedule.getOrElse("None")
                      )
                    )
                  )
                case None =>
                  TaskViewDependency(jobName, typ, "", UNKNOWN_TYPE, parentSQLRef)
              }
          }

        }
      }
    }
    val tableNames = jobAndViewDeps.filter(_.parentTyp == TABLE_TYPE).groupBy(_.parent).keys
    val tableDeps = tableNames.map(TaskViewDependency(_, TABLE_TYPE, "", UNKNOWN_TYPE, ""))
    val jobAndViewDepsWithSink = jobAndViewDeps.map { dep =>
      if (dep.typ == TASK_TYPE) {
        // TODO We just handle one task per job which is always the case till now.
        val task = jobs(dep.name).head
        val sink = task.taskDesc.domain + "." + task.taskDesc.table
        dep.copy(sink = Some(sink))
      } else dep

    }
    jobAndViewDepsWithSink ++ tableDeps
  }
}

case class TaskViewDependency(
  name: String,
  typ: String,
  parent: String,
  parentTyp: String,
  parentRef: String,
  sink: Option[String] = None,
  cron: Option[String] = None
) {

  @JsonIgnore
  def hasParent(): Boolean = parent.nonEmpty

  /** @return
    *   (bgColor, fontColor)
    */
  private def dotColor(): (String, String, String) = {
    import TaskViewDependency._
    typ match {
      case TASK_TYPE     => ("#00008B", "white", "Task") // darkblue
      case TASKVIEW_TYPE => ("darkcyan", "white", "TaskView")
      case VIEW_TYPE     => ("darkgrey", "white", "View")
      case TABLE_TYPE    => ("#008B00", "white", "Table")
      case CTE_TYPE      => ("darkorange", "white", "CTE")
      case UNKNOWN_TYPE  => ("black", "white", "???")
      case _             => throw new Exception(s"Unknown type $typ")
    }
  }

  def relationAsDot(): Option[String] = {
    val depId = name.replaceAll("\\.", "_")
    val dotParent: String = if (parent.isEmpty) parentRef else parent
    val dotParentId = dotParent.replaceAll("\\.", "_")
    if (dotParent.nonEmpty)
      Some(s"$dotParentId -> $depId")
    else
      None

  }

  def entityAsDot(): String = {
    val depId = name.replaceAll("\\.", "_")
    val (bgColor, fontColor, text) = dotColor()
    s"""
       |$depId [label=<
       |<table border="0" cellborder="1" cellspacing="0" cellpadding="10">
       |<tr>
       |<td bgcolor="$bgColor" ><font color="$fontColor" face="Arial">$text</font></td>
       |<td port="0" ><font face="Arial">$name&nbsp;&nbsp;</font>
       |</td></tr>
       |</table>>];""".stripMargin
  }
}
