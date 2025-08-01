package ai.starlake.schema.generator

import ai.starlake.config.{PrivacyLevels, Settings}
import ai.starlake.schema.model._
import ai.starlake.utils.TransformEngine
import org.apache.poi.ss.usermodel._

import java.io.File
import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer

sealed trait Input

case class InputPath(path: String) extends Input

case class InputFile(file: File) extends Input

final case class SchemaName(value: String) extends AnyVal

/** Reads the spreadsheet found at the specified {@param input} and builds the corresponding Domain
  * object
  *
  * @param input
  */
class XlsDomainReader(input: Input) extends XlsModel {

  private val workbook: Workbook = input match {
    case InputPath(s)  => WorkbookFactory.create(new File(s))
    case InputFile(in) => WorkbookFactory.create(in)
  }

  private lazy val domain: Option[DomainInfo] = {
    val sheet = Option(workbook.getSheet("_domain")).getOrElse(workbook.getSheet("domain"))
    val (rows, headerMap) = getColsOrder(sheet, allDomainHeaders.map { case (k, _) => k })
    rows.headOption.flatMap { row =>
      val nameOpt =
        Option(row.getCell(headerMap("_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val renameOpt =
        Option(row.getCell(headerMap("_rename"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val directoryOpt =
        Option(row.getCell(headerMap("_path"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      // Here for ack, we do not want to get None returned for an empty cell since None would give us a ".ack" as default later on
      val ack = Option(row.getCell(headerMap("_ack"), Row.MissingCellPolicy.CREATE_NULL_AS_BLANK))
        .flatMap(formatter.formatCellValue)
        .orElse(Some(""))
      val comment =
        Option(row.getCell(headerMap("_description"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val tags =
        Option(
          row.getCell(headerMap("_tags"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split(",").toSet).getOrElse(Set.empty)
      val dagRefOpt = Option(
        row.getCell(headerMap("_dagRef"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)
      val scheduleOpt =
        Option(
          row.getCell(headerMap("_frequency"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        )
          .flatMap(formatter.formatCellValue)

      val freshnessOpt = Option(
        row.getCell(headerMap("_freshness"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)

      val freshness = freshnessOpt
        .map(
          _.split(",")
            .flatMap { pair =>
              pair.split("=", 2) match {
                case Array(k, v) => Some(k -> v)
                case _           => None
              }
            }
            .toMap
        )
        .map { pairs =>
          val warn = pairs.get("warn").filter(_.nonEmpty)
          val error = pairs.get("error").filter(_.nonEmpty)
          Freshness(
            warn = warn,
            error = error
          )
        }
        .flatMap {
          case freshness if freshness.warn.isEmpty && freshness.error.isEmpty =>
            None
          case freshness if !freshness.checkValidity("test").toOption.getOrElse(false) =>
            None
          case other => Some(other)
        }

      nameOpt match {
        case Some(name) =>
          Some(
            DomainInfo(
              name,
              metadata = Some(
                Metadata(
                  directory = directoryOpt,
                  ack = ack,
                  dagRef = dagRefOpt,
                  schedule = scheduleOpt,
                  freshness = freshness
                )
              ),
              comment = comment,
              tags = tags,
              rename = renameOpt
            )
          )
        case _ => None
      }
    }
  }

  private lazy val schemas: List[(SchemaInfo, SchemaName)] = {
    val sheet = Option(workbook.getSheet("_schemas")).getOrElse(workbook.getSheet("schemas"))
    val (rows, headerMap) = getColsOrder(sheet, allSchemaHeaders.map { case (name, _) => name })
    rows.flatMap { row =>
      val nameOpt =
        Option(row.getCell(headerMap("_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val renameOpt =
        Option(row.getCell(headerMap("_rename"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val patternOpt =
        Option(row.getCell(headerMap("_pattern"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(Pattern.compile)
      val rawWrite =
        Option(row.getCell(headerMap("_write"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val scd2 = rawWrite.contains("SCD2")
      val write =
        rawWrite.map(WriteMode.fromString)
      val format =
        Option(row.getCell(headerMap("_format"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(Format.fromString)
      val withHeader =
        Option(row.getCell(headerMap("_header"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(_.toBoolean)
      val separator =
        Option(row.getCell(headerMap("_delimiter"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val deltaColOpt =
        Option(row.getCell(headerMap("_delta_column"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val identityKeysOpt =
        Option(row.getCell(headerMap("_merge_keys"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val comment =
        Option(row.getCell(headerMap("_description"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val encodingOpt =
        Option(row.getCell(headerMap("_encoding"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val partitionColumns =
        Option(row.getCell(headerMap("_partitioning"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(_.split(",") map (_.trim))
          .map(_.toList)
          .getOrElse(Nil)
      val sinkColumnsOpt =
        Option(row.getCell(headerMap("_sink"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val clusteringOpt =
        Option(row.getCell(headerMap("_clustering"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(_.split(","))
      val mergeQueryFilter =
        Option(
          row.getCell(headerMap("_merge_query_filter"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue)
      val presql =
        Option(
          row.getCell(headerMap("_presql"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split("###").toList).getOrElse(Nil)
      val postsql =
        Option(
          row.getCell(headerMap("_postsql"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split("###").toList).getOrElse(Nil)

      val primaryKeys =
        Option(
          row.getCell(headerMap("_primary_key"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split(",").toList).getOrElse(Nil)

      val tags =
        Option(
          row.getCell(headerMap("_tags"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        ).flatMap(formatter.formatCellValue).map(_.split(",").toSet).getOrElse(Set.empty)

      val longNameOpt =
        Option(row.getCell(headerMap("_long_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)

      val policiesOpt =
        Option(row.getCell(headerMap("_policy"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(_.split(",").map(_.trim))

      val escape =
        Option(row.getCell(headerMap("_escape"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)

      val quoteOpt =
        Option(row.getCell(headerMap("_quote"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellWithBlankValue)

      val nullValueOpt =
        Option(row.getCell(headerMap("_null"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)

      val dagRefOpt = Option(
        row.getCell(headerMap("_dagRef"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)

      val scheduleOpt =
        Option(
          row.getCell(headerMap("_frequency"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        )
          .flatMap(formatter.formatCellValue)

      val writeStrategy = (deltaColOpt, identityKeysOpt, mergeQueryFilter, write) match {
        case (Some(deltaCol), Some(identityKeys), filter, _) =>
          val strategyType =
            if (scd2)
              WriteStrategyType.SCD2
            else
              WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP
          WriteStrategy(
            `type` = Some(strategyType),
            key = identityKeys.split(",").toList.map(_.trim),
            timestamp = Some(deltaCol),
            queryFilter = filter
          )
        case (None, Some(identityKeys), filter, _) =>
          val strategyType =
            if (scd2)
              WriteStrategyType.SCD2
            else
              WriteStrategyType.UPSERT_BY_KEY
          WriteStrategy(
            `type` = Some(strategyType),
            key = identityKeys.split(",").toList.map(_.trim),
            queryFilter = filter
          )
        case (None, None, filter, Some(WriteMode.OVERWRITE)) =>
          if (partitionColumns.nonEmpty)
            WriteStrategy(
              `type` = Some(WriteStrategyType.OVERWRITE_BY_PARTITION),
              queryFilter = filter
            )
          else
            WriteStrategy(
              `type` = Some(WriteStrategyType.OVERWRITE),
              queryFilter = filter
            )
        case (_, _, _, Some(write)) =>
          WriteStrategy(`type` = Some(WriteStrategyType.fromWriteMode(write)))
        case (_, _, _, _) =>
          WriteStrategy(`type` = Some(WriteStrategyType.APPEND))
      }

      val sinkRes = sinkColumnsOpt
        .map(Sink.xlsfromConnectionType)
        .map {
          case fsSink: FsSink =>
            val clusteredFsSink = clusteringOpt match {
              case Some(cluster) => fsSink.copy(clustering = Some(cluster.toList))
              case None          => fsSink
            }
            val partition = partitionColumns match {
              case Nil => None
              case _ =>
                Some(partitionColumns)
            }
            clusteredFsSink.copy(partition = partition).toAllSinks()
          case bqSink: BigQuerySink =>
            val partitionBqSink = partitionColumns match {
              case ts :: Nil =>
                bqSink
                  .copy(partition = Some(List(ts))) // only one column allowed for BigQuery
              case Nil =>
                bqSink
              case _ =>
                throw new Exception("Only one partitioning column allowed for BigQuery")
            }
            val clusteredBqSink = clusteringOpt match {
              case Some(cluster) =>
                partitionBqSink.copy(clustering = Some(cluster.toList))
              case _ => partitionBqSink
            }
            clusteredBqSink.toAllSinks()
          case sink =>
            sink.toAllSinks()
        }

      val freshnessOpt = Option(
        row.getCell(headerMap("_freshness"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)

      val freshness = freshnessOpt
        .map(
          _.split(",")
            .flatMap { pair =>
              pair.split("=", 2) match {
                case Array(k, v) => Some(k -> v)
                case _           => None
              }
            }
            .toMap
        )
        .map { pairs =>
          val warn = pairs.get("warn").filter(_.nonEmpty)
          val error = pairs.get("error").filter(_.nonEmpty)
          Freshness(
            warn = warn,
            error = error
          )
        }
        .flatMap {
          case freshness if freshness.warn.isEmpty && freshness.error.isEmpty =>
            None
          case freshness if !freshness.checkValidity("test").toOption.getOrElse(false) =>
            None
          case other => Some(other)
        }

      (nameOpt, patternOpt) match {
        case (Some(name), Some(pattern)) => {
          val metaData = Metadata(
            format = format,
            encoding = encodingOpt,
            multiline = None,
            array = None,
            withHeader = withHeader,
            separator = separator,
            escape = escape,
            sink = sinkRes,
            writeStrategy = Some(writeStrategy),
            quote = quoteOpt,
            nullValue = nullValueOpt,
            dagRef = dagRefOpt,
            schedule = scheduleOpt,
            freshness = freshness
          )

          val tablePolicies = policiesOpt
            .map { tablePolicies =>
              policies.filter(p => tablePolicies.contains(p.name))
            }
            .getOrElse(Nil)

          val acl = tablePolicies.flatMap {
            case sl: AccessControlEntry => Some(sl)
            case _                      => None
          }
          val rls = tablePolicies.flatMap {
            case sl: RowLevelSecurity => Some(sl)
            case _                    => None
          }

          val schema = SchemaInfo(
            name = longNameOpt.getOrElse(name),
            pattern = pattern,
            attributes = Nil,
            metadata = Some(metaData),
            comment = comment,
            presql = presql,
            postsql = postsql,
            tags = tags,
            primaryKey = primaryKeys,
            rename = renameOpt,
            acl = acl,
            rls = rls
          )
          Some(schema, SchemaName(name))
        }
        case _ => None
      }
    }.toList
  }

  private lazy val policies: List[SecurityLevel] = {
    val sheet = Option(workbook.getSheet("_policies")).getOrElse(workbook.getSheet("policies"))
    val (rows, headerMap) = getColsOrder(sheet, allPolicyHeaders.map { case (k, _) => k })
    rows.flatMap { row =>
      val nameOpt =
        Option(row.getCell(headerMap("_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val predicateOpt =
        Option(row.getCell(headerMap("_predicate"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val grantsOpt =
        Option(row.getCell(headerMap("_grants"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val descriptionOpt =
        Option(row.getCell(headerMap("_description"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val aclOpt =
        Option(row.getCell(headerMap("_acl"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .forall(_.toBoolean)
      val rlsOpt =
        Option(row.getCell(headerMap("_rls"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .forall(_.toBoolean)
      (nameOpt, predicateOpt, grantsOpt, descriptionOpt, aclOpt, rlsOpt) match {
        case (Some(name), predicate, Some(grants), description, aclOpt, rlsOpt) =>
          val _grants = grants
            .replaceAll("\\s*,\\s*", ",")
            .replaceAll("\\s+", ",")
            .split(',')
            .toSet
          val _predicate = predicate.getOrElse("TRUE")
          val acl = AccessControlEntry("roles/bigquery.dataViewer", _grants, name)
          val rls = RowLevelSecurity(
            name = name,
            predicate = _predicate,
            grants = _grants,
            description = description.getOrElse("")
          )
          if (aclOpt && rlsOpt) {
            List(
              rls,
              acl
            )
          } else if (aclOpt) {
            Some(acl)
          } else if (rlsOpt) {
            Some(rls)
          } else if (_predicate.toUpperCase() == "TRUE") { // for backward compatibility
            Some(acl)
          } else {
            Some(rls)
          }
        case _ => None
      }
    }.toList
  }
  private lazy val iamPolicyTags: List[IamPolicyTag] = {
    val sheet =
      Option(workbook.getSheet("_iam_policy_tags")).getOrElse(workbook.getSheet("iam_policy_tags"))
    val (rows, headerMap) = getColsOrder(sheet, allIamPolicyTagHeaders.map { case (k, _) => k })
    rows.flatMap { row =>
      val policyTagOpt =
        Option(row.getCell(headerMap("_policyTag"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
      val membersOpt =
        Option(row.getCell(headerMap("_members"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)
          .map(_.split(","))
      val roleOpt =
        Option(row.getCell(headerMap("_role"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
          .flatMap(formatter.formatCellValue)

      (policyTagOpt, membersOpt, roleOpt) match {
        case (Some(policyTag), Some(members), Some(role)) =>
          Some(
            IamPolicyTag(
              policyTag,
              members.toList,
              roleOpt
            )
          )
        case _ => None
      }
    }.toList
  }

  def getIamPolicyTags(): List[IamPolicyTag] = iamPolicyTags

  /** Returns the Domain corresponding to the parsed spreadsheet
    * @param settings
    * @return
    *   an Option of Domain
    */
  def getDomain()(implicit settings: Settings): Option[DomainInfo] = {
    val completeSchemas = buildSchemas().filter(_.attributes.nonEmpty)
    domain.map(_.copy(tables = completeSchemas))
  }

  private def buildSchemas()(implicit settings: Settings): List[SchemaInfo] = {
    schemas.map { case (schema, originalName) =>
      val schemaName = originalName.value
      val sheetOpt = Option(workbook.getSheet(schemaName))
      val attributes = sheetOpt match {
        case None => List.empty
        case Some(sheet) =>
          buildAttributes(schema, sheet)
      }
      schema.copy(attributes = attributes)
    }
  }

  private def buildAttributes(schema: SchemaInfo, sheet: Sheet)(implicit
    settings: Settings
  ): List[TableAttribute] = {
    val (rows, headerMap) = getColsOrder(sheet, allAttributeHeaders.map { case (k, _) => k })
    val attrs = rows.flatMap { row =>
      readAttribute(schema, headerMap, row)
    }.toList
    val withEndOfStruct = markEndOfStruct(attrs)
    val topParent = TableAttribute("__dummy", "struct", attributes = withEndOfStruct)
    buildAttrsTree(topParent, withEndOfStruct.iterator).attributes
  }

  private def markEndOfStruct(attrs: List[TableAttribute]): List[TableAttribute] = {
    var previousLevel = 0
    val result = attrs.flatMap { attr =>
      val level = attr.name.count(_ == '.')
      val attrWithEndStruct: List[TableAttribute] =
        if (level == previousLevel)
          List(attr)
        else if (level == previousLevel + 1) {
          List(attr)
        } else if (level < previousLevel) {
          val endingStruct = ListBuffer.empty[TableAttribute]
          for (i <- level until previousLevel)
            endingStruct.append(TableAttribute(name = "__end_struct", `type` = "struct"))
          endingStruct.toList :+ attr
        } else
          throw new Exception("Invalid Level in XLS")
      previousLevel = level
      attrWithEndStruct
    }
    val endingStruct = ListBuffer.empty[TableAttribute]
    for (i <- 0 until previousLevel)
      endingStruct.append(TableAttribute(name = "__end_struct", `type` = "struct"))

    result ++ endingStruct.toList
  }

  private def buildAttrsTree(
    parent: TableAttribute,
    attributes: Iterator[TableAttribute]
  )(implicit settings: Settings): TableAttribute = {
    //
    val attrsAtTheSameLevel: ListBuffer[TableAttribute] = ListBuffer.empty
    var endOfStructFound = false
    while (!endOfStructFound && attributes.hasNext) {
      val attr = attributes.next()
      val attrName = attr.name.split('.').last
      val finalAttr = attr.copy(name = attrName)
      val attrTree = finalAttr.`type` match {
        case "struct" if finalAttr.name == "__end_struct" =>
          endOfStructFound = true
          parent.copy(attributes = attrsAtTheSameLevel.toList)

        case "struct" =>
          val childAttr = buildAttrsTree(finalAttr, attributes)
          attrsAtTheSameLevel.append(childAttr)
          childAttr
        case _ =>
          attrsAtTheSameLevel.append(finalAttr)
          finalAttr
      }
    }
    parent.copy(attributes = attrsAtTheSameLevel.toList)
  }

  private def readAttribute(
    schema: SchemaInfo,
    headerMap: Map[String, Int],
    row: Row
  )(implicit settings: Settings): Option[TableAttribute] = {
    val nameOpt =
      Option(row.getCell(headerMap("_name"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
    val renameOpt =
      Option(row.getCell(headerMap("_rename"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
    val semTypeOpt =
      Option(row.getCell(headerMap("_type"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
    val required = Option(
      row.getCell(headerMap("_required"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
    ).flatMap(formatter.formatCellValue)
      .forall(_.toBoolean)
    val privacy =
      Option(row.getCell(headerMap("_privacy"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
        .map { value =>
          val allPrivacyLevels =
            PrivacyLevels.allPrivacyLevels(settings.appConfig.privacy.options)
          val ignore: Option[((TransformEngine, List[String]), TransformInput)] =
            allPrivacyLevels.get(value.toUpperCase)
          ignore.map { case (_, level) => level }.getOrElse {
            if (value.toUpperCase().startsWith("SQL:"))
              TransformInput(value.substring("SQL:".length), true)
            else
              throw new Exception(s"key not found: $value")
          }
        }
    val metricType =
      Option(row.getCell(headerMap("_metric"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
        .map(MetricType.fromString)
    val defaultOpt =
      Option(row.getCell(headerMap("_default"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
    val scriptOpt =
      Option(row.getCell(headerMap("_script"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
    val commentOpt = Option(
      row.getCell(headerMap("_description"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
    ).flatMap(formatter.formatCellValue)

    val positionOpt = schema.metadata.flatMap(_.format) match {
      case Some(Format.POSITION) => {
        val positionStart =
          Option(
            row.getCell(
              headerMap("_position_start"),
              Row.MissingCellPolicy.RETURN_BLANK_AS_NULL
            )
          ).flatMap(formatter.formatCellValue)
            .map(_.toInt) match {
            case Some(v) => v - 1
            case _       => 0
          }
        val positionEnd =
          Option(
            row.getCell(
              headerMap("_position_end"),
              Row.MissingCellPolicy.RETURN_BLANK_AS_NULL
            )
          ).flatMap(formatter.formatCellValue)
            .map(_.toInt) match {
            case Some(v) => v - 1
            case _       => 0
          }
        Some(Position(positionStart, positionEnd))
      }
      case _ => None
    }
    val attributeTrim =
      Option(row.getCell(headerMap("_trim"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
        .map(Trim.fromString)

    val attributeIgnore =
      Option(row.getCell(headerMap("_ignore"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL))
        .flatMap(formatter.formatCellValue)
        .map(_.toBoolean)

    val foreignKey =
      Option(
        row.getCell(headerMap("_foreign_key"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue)

    val tags =
      Option(
        row.getCell(headerMap("_tags"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
      ).flatMap(formatter.formatCellValue).map(_.split(",").toSet).getOrElse(Set.empty)

    val accessPolicy = Option(
      row.getCell(headerMap("_policy"), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
    ).flatMap(formatter.formatCellValue)

    (nameOpt, semTypeOpt) match {
      case (Some(name), Some(semanticType)) =>
        val isArray = if (name.endsWith("*")) Some(true) else None
        val simpleName = if (isArray.getOrElse(false)) name.dropRight(1) else name
        Some(
          TableAttribute(
            name = simpleName,
            `type` = semanticType,
            array = isArray,
            required = Some(required),
            privacy,
            comment = commentOpt,
            rename = renameOpt,
            metricType = metricType,
            trim = attributeTrim,
            position = positionOpt,
            default = defaultOpt,
            script = scriptOpt,
            attributes = Nil,
            ignore = attributeIgnore,
            foreignKey = foreignKey,
            tags = tags,
            accessPolicy = accessPolicy
          )
        )
      case _ => None
    }
  }
}
