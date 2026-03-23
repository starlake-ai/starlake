package ai.starlake.schema.handlers

import ai.starlake.schema.model._
import ai.starlake.utils.CaseInsensitiveMap

object SchedulingQueries {

  def streams(
    domains: List[DomainInfo],
    jobs: List[AutoJobInfo]
  ): CaseInsensitiveMap[String] = {
    val tableStreams =
      domains.flatMap { domain =>
        domain.tables.flatMap { table =>
          table.streams.map { stream =>
            stream -> s"${domain.finalName}.${table.finalName}"
          }
        }
      }.toMap

    val taskStreams =
      jobs.flatMap { job =>
        job.tasks.flatMap { task =>
          task.streams.map { stream =>
            stream -> s"${job.getName()}.${task.getTableName()}"
          }
        }
      }.toMap

    CaseInsensitiveMap(tableStreams ++ taskStreams)
  }

  def orderSchedules(
    orderBy: Option[String],
    schedules: List[ObjectSchedule]
  ): List[ObjectSchedule] = {
    orderBy match {
      case Some("name") =>
        schedules.sortBy(s => s"${s.domain}.${s.table}")
      case Some("cron") =>
        schedules.sortBy(_.cron)
      case _ =>
        schedules
    }
  }

  def taskSchedules(
    jobs: List[AutoJobInfo],
    orderBy: Option[String]
  ): List[ObjectSchedule] = {
    val schedules = jobs.flatMap { job =>
      job.tasks.map { task =>
        ObjectSchedule(
          domain = job.getName(),
          table = task.getTableName(),
          cron = task.schedule,
          comment = task.comment,
          typ = "task"
        )
      }
    }
    orderSchedules(orderBy, schedules)
  }

  def tableSchedules(
    domains: List[DomainInfo],
    orderBy: Option[String]
  ): List[ObjectSchedule] = {
    val schedules = domains.flatMap { domain =>
      domain.tables.map { table =>
        val metadata = table.metadata.getOrElse(Metadata())
        ObjectSchedule(
          domain = domain.finalName,
          table = table.finalName,
          cron = metadata.schedule,
          comment = table.comment,
          typ = "table"
        )
      }
    }
    orderSchedules(orderBy, schedules)
  }

  def allSchedules(
    jobs: List[AutoJobInfo],
    domains: List[DomainInfo],
    orderBy: Option[String]
  ): List[ObjectSchedule] = {
    val taskSchedulesList = taskSchedules(jobs, orderBy)
    val tableSchedulesList = tableSchedules(domains, orderBy)
    val schedules = taskSchedulesList ++ tableSchedulesList
    orderSchedules(orderBy, schedules)
  }
}
