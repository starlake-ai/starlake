package ai.starlake.schema

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.FileInfo
import ai.starlake.schema.model.WriteStrategyType._
import ai.starlake.schema.model.{Schema, WriteStrategyType}
import ai.starlake.utils.CompilerUtils
import com.typesafe.scalalogging.LazyLogging

import java.time.ZonedDateTime
import java.time.temporal.{ChronoField, TemporalAdjusters}
import java.util.regex.Matcher
import scala.util.{Failure, Success, Try}

object AdaptiveWriteStrategy extends LazyLogging {

  /** Adapt schema based on file info and write strategy defined in schema. Since schema may change,
    * sub-lists of same schema are created by preserving input sequence, allowing to execute grouped
    * ingestion accordingly.
    */
  def adaptThenGroup(
    schema: Schema,
    fileInfos: Iterable[FileInfo]
  )(implicit settings: Settings): Seq[(Schema, Iterable[FileInfo])] = {
    schema.metadata.flatMap(_.writeStrategy).flatMap(_.types) match {
      case Some(writeStrategyTypes) =>
        val results = fileInfos.toList
          .map(f => adapt(schema, f, writeStrategyTypes) -> f)
          .reverse
          .foldLeft(List[(Schema, List[FileInfo])]()) {
            case ((schema, fileInfos) +: groupedSchemas, (newSchema, newFileInfo))
                if schema == newSchema =>
              (schema, newFileInfo +: fileInfos) +: groupedSchemas
            case (groupedSchemas, (schema, fileInfo)) =>
              schema -> List(fileInfo) :: groupedSchemas
          }
        val adaptedStrategies = results
          .map { case (schema, _) =>
            schema.metadata
              .flatMap(_.writeStrategy)
              .flatMap(_.`type`)
              .map(_.value)
              .getOrElse("UNDEFINED_STRATEGY") // should not happen
          }
          .mkString(", ")
        logger.info(
          s"Schema has been adapted for ${schema.name} and produced ${results.length} subgroups with the following strategies $adaptedStrategies."
        )
        results
      case _ =>
        List(
          schema -> fileInfos
        ) // we don't have any write strategy defined so schema can't be altered
    }
  }

  /** Alter given schema based on defined write strategy. Apply one of the write strategy only. If
    * not the case, throw an exception.
    */
  def adapt(schema: Schema, fileInfo: FileInfo, writeStrategy: Map[String, String])(implicit
    settings: Settings
  ): Schema = {
    val matcher: Matcher = schema.pattern.matcher(fileInfo.path.getName)
    matcher.find()
    val context = buildContext(fileInfo, matcher)
    writeStrategy.filter { case (_, condition) =>
      Try {
        CompilerUtils.compileWriteStrategy(condition)(context)
      } match {
        case Failure(exception) =>
          logger.error(
            "An exception occured during strategy choice. Please ensure that your expression is correct.",
            exception
          )
          throw exception
        case Success(value) => value
      }
    }.toList match {
      case (strategy, _) +: Nil =>
        val sinksOpt = schema.metadata.flatMap(_.sink)
        strategy match {
          case APPEND.value =>
            schema.copy(
              metadata = schema.metadata.map(m =>
                m.copy(
                  sink = m.sink,
                  writeStrategy = m.writeStrategy.map(
                    _.copy(`type` = Some(WriteStrategyType.fromString(strategy)))
                  )
                )
              )
            )
          case OVERWRITE.value =>
            schema.copy(
              metadata = schema.metadata.map(m =>
                m.copy(
                  sink = m.sink,
                  writeStrategy = m.writeStrategy.map(
                    _.copy(`type` = Some(WriteStrategyType.fromString(strategy)))
                  )
                )
              )
            )
          case UPSERT_BY_KEY.value =>
            val key = schema.metadata.flatMap(_.writeStrategy.map(_.key)).getOrElse(Nil)
            require(
              key.nonEmpty,
              s"Using UPSERT_BY_KEY for schema ${schema.name} requires to set merge.key on it"
            )
            schema.copy(
              metadata = schema.metadata.map(m =>
                m.copy(
                  sink = m.sink,
                  writeStrategy = m.writeStrategy.map(
                    _.copy(`type` = Some(WriteStrategyType.fromString(strategy)))
                  )
                )
              )
            )
          case UPSERT_BY_KEY_AND_TIMESTAMP.value =>
            val key = schema.metadata.flatMap(_.writeStrategy.map(_.key)).getOrElse(Nil)
            val timestamp = schema.metadata.flatMap(_.writeStrategy.flatMap(_.timestamp))

            require(
              key.nonEmpty && timestamp.nonEmpty,
              s"Using ${UPSERT_BY_KEY_AND_TIMESTAMP.value} for schema ${schema.name} requires to set merge.key and merge.timestamp on it"
            )
            schema.copy(
              metadata = schema.metadata.map(m =>
                m.copy(
                  sink = m.sink,
                  writeStrategy = m.writeStrategy.map(
                    _.copy(`type` = Some(WriteStrategyType.fromString(strategy)))
                  )
                )
              )
            )
          case OVERWRITE_BY_PARTITION.value =>
            val connectionTypeOpt = sinksOpt.map(_.getSink().getConnectionType())
            val partition = sinksOpt.flatMap(_.partition).getOrElse(Nil)
            require(
              partition.nonEmpty,
              s"Using ${OVERWRITE_BY_PARTITION.value} for schema ${schema.name} requires to one of merge.key, metadata.sink.timestamp or metadata.sink.partition depending on connection type."
            )
            schema.copy(
              metadata = schema.metadata.map(m =>
                m.copy(
                  sink = m.sink,
                  writeStrategy = m.writeStrategy.map(
                    _.copy(`type` = Some(WriteStrategyType.fromString(strategy)))
                  )
                )
              )
            )
          case SCD2.value =>
            throw new NotImplementedError(s"${SCD2.value} is not implemented yet.")
          case strategyName =>
            throw new RuntimeException(
              s"Adaptive write strategy currently doesn't support custom strategy: $strategyName"
            )
        }
      case l if l.size > 1 =>
        throw new RuntimeException(
          s"${schema.name} is eligible to ${l.size} write strategies with\n\t- path: ${fileInfo.path}\n\t - file size: ${fileInfo.fileSizeInBytes} bytes"
        )
      case _ =>
        throw new RuntimeException(
          s"Could not find valid strategy for ${schema.name} with\n\t- path: ${fileInfo.path}\n\t - file size: ${fileInfo.fileSizeInBytes} bytes"
        )
    }
  }

  private def buildContext(fileInfo: FileInfo, matcher: Matcher)(implicit
    settings: Settings
  ): Map[String, Any] = {
    val currentZonedDateTime = ZonedDateTime.now(settings.appConfig.timezone.toZoneId)
    val fileZonedDateTime =
      fileInfo.modificationInstant.atZone(settings.appConfig.timezone.toZoneId)
    Map(
      "matcher"               -> matcher,
      "fileSize"              -> fileInfo.fileSizeInBytes,
      "isFirstDayOfMonth"     -> isFirstDayOfMonth(currentZonedDateTime),
      "isLastDayOfMonth"      -> isLastDayOfMonth(currentZonedDateTime),
      "dayOfWeek"             -> dayOfWeek(currentZonedDateTime),
      "isFileFirstDayOfMonth" -> isFirstDayOfMonth(fileZonedDateTime),
      "isFileLastDayOfMonth"  -> isLastDayOfMonth(fileZonedDateTime),
      "fileDayOfWeek"         -> dayOfWeek(fileZonedDateTime)
    )
  }

  private def isFirstDayOfMonth(zonedDateTime: ZonedDateTime): Boolean = {
    zonedDateTime.get(ChronoField.DAY_OF_MONTH) == 1
  }

  private def isLastDayOfMonth(zonedDateTime: ZonedDateTime): Boolean = {
    val currentDay = zonedDateTime.get(ChronoField.DAY_OF_MONTH)
    val lastDayOfMonth =
      zonedDateTime.`with`(TemporalAdjusters.lastDayOfMonth()).get(ChronoField.DAY_OF_MONTH)
    currentDay == lastDayOfMonth
  }

  private def dayOfWeek(zonedDateTime: ZonedDateTime): Int = {
    zonedDateTime.get(ChronoField.DAY_OF_WEEK)
  }
}
