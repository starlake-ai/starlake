package ai.starlake.job.transform

import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Domain
import ai.starlake.utils.SQLUtils
import com.typesafe.scalalogging.StrictLogging

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

  case class SimpleEntry(name: String, typ: String, parentRefs: List[String])
  def jobDependencies(jobName: String, tasks: List[AutoTask])(implicit
    schemaHandler: SchemaHandler
  ): List[TaskViewDependency] = {
    val deps = dependencies(tasks)
    var result = scala.collection.mutable.ListBuffer[TaskViewDependency]()
    val roots = deps.filter(t => t.name == jobName && t.typ == TASK_TYPE)
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
      val subRoots = allDeps.filter(t => t.typ == root.parentTyp && t.name == root.parent)
      val nocyclicRoots = subRoots.filter(subRoot =>
        !roots.exists(_.name.toLowerCase() == subRoot.name.toLowerCase())
      )
      result ++= nocyclicRoots
      getHierarchy(nocyclicRoots, allDeps, result)
    }
  }

  def dependencies(
    tasks: List[AutoTask]
  )(implicit schemaHandler: SchemaHandler): List[TaskViewDependency] = {
    val jobs: Map[String, List[AutoTask]] = tasks.groupBy(_.name)
    val jobDependencies: List[SimpleEntry] =
      jobs.mapValues(_.flatMap(_.dependencies())).toList.map { case (jobName, dependencies) =>
        SimpleEntry(jobName, TASK_TYPE, dependencies)
      }
    val viewDependencies: List[SimpleEntry] =
      schemaHandler
        .views()
        .mapValues(SQLUtils.extractRefsFromSQL)
        .map { case (viewName, dependencies) =>
          SimpleEntry(viewName, VIEW_TYPE, dependencies)
        }
        .toList

    val jobAndViewDeps = (jobDependencies ++ viewDependencies).flatMap {
      case SimpleEntry(jobName, typ, parentRefs) =>
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
                tasks.find(_.taskDesc.table.toLowerCase() == tablePart.toLowerCase()).map(_.name)

              case 2 | 3 =>
                val domainPart = parts.dropRight(1).last
                val tablePart = parts.last
                tasks
                  .find(task =>
                    task.taskDesc.table.toLowerCase() == tablePart
                      .toLowerCase() && task.taskDesc.domain
                      .toLowerCase() == domainPart.toLowerCase()
                  )
                  .map(_.name)
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
              case Some(parentJobName) =>
                val result =
                  TaskViewDependency(jobName, typ, parentJobName, TASK_TYPE, parentSQLRef)
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
                    val parentDomain: Option[Domain] = domains
                      .find(domain =>
                        domain.tables.exists(_.name.toLowerCase() == tablePart.toLowerCase())
                      )
                    parentDomain.map(domain => (domain.name, tablePart))

                  case 2 | 3 =>
                    val domainPart = parts.dropRight(1).last
                    val tablePart = parts.last
                    val theDomain: Option[Domain] = domains
                      .find(_.name.toLowerCase() == domainPart.toLowerCase())
                    val parentDomainFound = theDomain.exists(
                      _.tables.exists(table => table.name.toLowerCase() == tablePart.toLowerCase())
                    )
                    if (parentDomainFound)
                      Some((domainPart, tablePart))
                    else
                      None
                  case _ =>
                    // Strange. This should never happen as far as I know. Let's log it
                    throw new Exception(s"unknown $parentSQLRef syntax. Too many parts")
                }
                parentTable match {
                  case Some((parentDomainName, parentTableName)) =>
                    TaskViewDependency(
                      jobName,
                      typ,
                      parentDomainName + "." + parentTableName,
                      TABLE_TYPE,
                      parentSQLRef
                    )
                  case None =>
                    val parentViewNameExist: Option[Boolean] = jobs
                      .get(parentSQLRef)
                      .map(_.exists(_.views.views.keys.exists(_.toLowerCase() == parentSQLRef)))
                    parentViewNameExist match {
                      case Some(true) =>
                        TaskViewDependency(jobName, typ, parentSQLRef, TASKVIEW_TYPE, parentSQLRef)
                      case None | Some(false) =>
                        val found =
                          schemaHandler
                            .views()
                            .keys
                            .exists(_.toLowerCase() == parentSQLRef.toLowerCase())
                        if (found)
                          TaskViewDependency(jobName, typ, parentSQLRef, VIEW_TYPE, parentSQLRef)
                        else
                          TaskViewDependency(jobName, typ, "", UNKNOWN_TYPE, parentSQLRef)
                    }
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
  sink: Option[String] = None
) {
  def hasParent(): Boolean = parent.nonEmpty

  private def dotBgColor() = {
    import TaskViewDependency._
    typ match {
      case TASK_TYPE     => "darkgreen"
      case TASKVIEW_TYPE => "darkcyan"
      case VIEW_TYPE     => "darkblue"
      case TABLE_TYPE    => "black"
      case CTE_TYPE      => "darkorange"
      case UNKNOWN_TYPE  => "darkgrey"
      case _             => throw new Exception(s"Unknown type $typ")
    }
  }

  def relationAsDot() = {
    val depId = name.replaceAll("\\.", "_")
    val dotParent: String = if (parent.isEmpty) parentRef else parent
    val dotParentId = dotParent.replaceAll("\\.", "_")
    if (dotParent.nonEmpty)
      Some(s"$depId -> $dotParentId")
    else
      None

  }

  def entityAsDot(): String = {
    val depId = name.replaceAll("\\.", "_")
    val depBgColor = dotBgColor()
    val sinkToTable = sink match {
      case None => ""
      case Some(sink) =>
        s"""<tr><td port="2">&#8594;$sink&nbsp;&nbsp;</td></tr>"""
    }
    s"""
       |$depId [label=<
       |<table border="0" cellborder="1" cellspacing="0">
       |<tr><td port="0" bgcolor="$depBgColor"><B><FONT color="white"> ${typ.capitalize} </FONT></B></td></tr>
       |<tr><td port="1">$name</td></tr>
       |$sinkToTable
       |     </table>>];""".stripMargin
  }
}
