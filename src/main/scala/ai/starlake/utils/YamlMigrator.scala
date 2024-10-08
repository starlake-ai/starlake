package ai.starlake.utils
import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.schema.model.WriteStrategyType
import ai.starlake.utils.YamlSerde.mapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{
  ArrayNode,
  BooleanNode,
  IntNode,
  ObjectNode,
  TextNode,
  ValueNode
}
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}

import scala.jdk.CollectionConverters._

trait YamlUtils {

  /** Syntax for path is :
    *   - nodeName in order to access it
    *   - . is the separator used between node names
    *   - [*] to iterate through array node and rename all elements inside it if path doesn't exist,
    *     don't do anything.
    */
  protected def transformNode(
    nodePath: String,
    transform: JsonNode => JsonNode
  ): JsonNode => JsonNode = {
    case node if nodePath.isEmpty =>
      transform(node)
    case node: ObjectNode if node.has(nodePath) =>
      node.set(nodePath, transform(node.path(nodePath)))
    case node =>
      val (nextPath, tailPathWithSeparator) = nodePath.span(_ != '.')
      val tailPath =
        if (tailPathWithSeparator.isEmpty) tailPathWithSeparator else tailPathWithSeparator.tail
      (node, nextPath) match {
        case _ if node.has(nextPath) =>
          transformNode(tailPath, transform)(node.path(nextPath))
          node
        case (an: ArrayNode, "[*]") =>
          val newElements = an.asScala.map { element =>
            transformNode(tailPath, transform)(element)
          }.asJavaCollection
          an.removeAll()
          an.addAll(newElements)
        case _ =>
          node
      }
  }

  /** Syntax for path is :
    *   - nodeName in order to access it
    *   - . is the separator used between node names
    *   - [*] to iterate through array node and rename all elements inside it if path doesn't exist,
    *     don't do anything.
    */
  protected def renameField(fieldPath: String, newName: String): JsonNode => JsonNode = {
    case node: ObjectNode if node.has(fieldPath) =>
      val oldNode = node.path(fieldPath)
      val newNode = node.path(newName)
      if ((newNode.isNull || newNode.isMissingNode) && !(oldNode.isNull || oldNode.isMissingNode)) {
        node.remove(fieldPath)
        node.set(newName, oldNode)
      }
      node
    case node =>
      val (nextPath, tailPathWithSeparator) = fieldPath.span(_ != '.')
      val tailPath =
        if (tailPathWithSeparator.isEmpty) tailPathWithSeparator else tailPathWithSeparator.tail
      (node, nextPath) match {
        case _ if node.has(nextPath) =>
          renameField(tailPath, newName)(node.path(nextPath))
          node
        case (an: ArrayNode, "[*]") =>
          val arrayNode = mapper.createArrayNode()
          arrayNode.addAll(an.asScala.map { element =>
            renameField(tailPath, newName)(element)
          }.asJavaCollection)
          arrayNode
        case _ =>
          node
      }
  }

  /** Syntax for path is :
    *   - nodeName in order to access it
    *   - . is the separator used between node names
    *   - [*] to iterate through array node and rename all elements inside it if path doesn't exist,
    *     don't do anything.
    */
  protected def removeField(fieldPath: String): JsonNode => JsonNode = {
    case node: ObjectNode if node.has(fieldPath) =>
      val oldNode = node.path(fieldPath)
      node.remove(fieldPath)
      node
    case node =>
      val (nextPath, tailPathWithSeparator) = fieldPath.span(_ != '.')
      val tailPath =
        if (tailPathWithSeparator.isEmpty) tailPathWithSeparator else tailPathWithSeparator.tail
      (node, nextPath) match {
        case _ if node.has(nextPath) =>
          removeField(tailPath)(node.path(nextPath))
          node
        case (an: ArrayNode, "[*]") =>
          val arrayNode = mapper.createArrayNode()
          arrayNode.addAll(an.asScala.map { element =>
            removeField(tailPath)(element)
          }.asJavaCollection)
          arrayNode
        case _ =>
          node
      }
  }

  protected def wrapToContainer(containerName: String): JsonNode => JsonNode = {
    case n if n.has(containerName) => n
    case n                         => YamlSerde.mapper.createObjectNode().set(containerName, n)
  }
}

trait YamlMigratorInterface extends StrictLogging with YamlUtils {

  private val versionFieldName = "version"

  val targetVersion: Int

  def canMigrate(node: JsonNode): Boolean

  def migrate(node: JsonNode): JsonNode = changes.foldLeft(node) { case (newNode, changeFunc) =>
    changeFunc(newNode)
  }

  protected def changes: List[JsonNode => JsonNode]

  protected def getVersion(node: JsonNode): Option[Int] = {
    Option(node.get(versionFieldName)) match {
      case Some(value: IntNode) => Some(value.intValue())
      case None                 => None
      case _ =>
        throw new RuntimeException(
          s"$versionFieldName is expected to be an int node but found ${node.getClass.getName}"
        )
    }
  }

  protected def setVersion(version: Int): JsonNode => JsonNode = {
    case on: ObjectNode => on.set(versionFieldName, IntNode.valueOf(version))
    case node =>
      throw new RuntimeException(
        s"Expecting to set $versionFieldName on object node but found ${node.getClass.getName}"
      )
  }

  protected def applyMigrationOn(
    chrootNodeRetriever: JsonNode => JsonNode,
    migrationList: List[JsonNode => JsonNode]
  ): List[JsonNode => JsonNode] = {
    migrationList.map(f =>
      (rootNode: JsonNode) => {
        chrootNodeRetriever(rootNode) match {
          case an: ArrayNode =>
            val result = an.asScala.map(f).asJavaCollection
            an.removeAll()
            an.addAll(result)
          case node => f(node)
        }
        rootNode
      }
    )
  }

  protected def keepFirst(
    chrootNodeRetriever: JsonNode => JsonNode
  ): JsonNode => JsonNode = { (rootNode: JsonNode) =>
    {
      chrootNodeRetriever(rootNode) match {
        case an: ArrayNode =>
          if (an.size() > 1) {
            logger.warn(s"Keeping first element out of ${an.size()} in array.")
          }
          (1 until an.size()).foreach(an.remove)
        case _ => // do nothing
      }
      rootNode
    }
  }

  protected def applyMigrationOnP1(
    chrootNodeRetriever: JsonNode => JsonNode,
    param1Retriever: JsonNode => JsonNode,
    migrationList: List[JsonNode => JsonNode => JsonNode]
  ): List[JsonNode => JsonNode] = {

    migrationList.map(f =>
      (rootNode: JsonNode) => {
        chrootNodeRetriever(rootNode) match {
          case an: ArrayNode =>
            val result = an.asScala.map(f(param1Retriever(rootNode))).asJavaCollection
            an.removeAll()
            an.addAll(result)
          case node => f(param1Retriever(rootNode))(node)
        }
        rootNode
      }
    )
  }

  protected def applyMigration(
    migrationList: List[JsonNode => JsonNode]
  ): List[JsonNode => JsonNode] = applyMigrationOn(identity, migrationList)
}

object YamlMigrator extends LazyLogging {

  object PreV1 {
    trait YamlMigratorInterfacePreV1 extends YamlMigratorInterface {

      override val targetVersion: Int = 0
      override def canMigrate(node: JsonNode): Boolean = getVersion(node).isEmpty
    }

    object TableConfig extends YamlMigratorInterfacePreV1 {
      override val changes: List[JsonNode => JsonNode] = List(
        renameField("schema", "table"),
        renameField("schemas", "tables")
      )
    }
  }
  object V1 {

    trait WriteMigrator extends YamlMigratorInterface {
      protected val migrateWriteMetadataNode: List[JsonNode => JsonNode] =
        List[JsonNode => JsonNode] {
          case writeContainerNode: ObjectNode =>
            if (
              !(writeContainerNode
                .path("writeStrategy")
                .hasNonNull("type") || writeContainerNode.path("writeStrategy").hasNonNull("types"))
            ) {
              writeContainerNode.path("write") match {
                case tn: TextNode =>
                  val writeMode = tn.textValue().toUpperCase match {
                    case "OVERWRITE" => Some(WriteStrategyType.OVERWRITE.value)
                    case "APPEND"    => Some(WriteStrategyType.APPEND.value)
                    case mode if mode.nonEmpty =>
                      Some(WriteStrategyType.APPEND.value)
                  }
                  writeMode.foreach { wm =>
                    val objectNode = mapper
                      .createObjectNode()
                      .set[ObjectNode](
                        "type",
                        TextNode.valueOf(wm)
                      )
                    writeContainerNode.set[ObjectNode]("writeStrategy", objectNode)
                  }
                  writeContainerNode
                case _ => writeContainerNode
              }
            } else writeContainerNode
          case n => n
        }
    }

    trait WriteStrategyTypesMigrator extends YamlMigratorInterface {
      protected val migrateWriteStrategyTypesMetadataNode: List[JsonNode => JsonNode] =
        List[JsonNode => JsonNode] {
          case writeContainerNode: ObjectNode =>
            writeContainerNode.path("writeStrategy") match {
              case writeStrategyNode: ObjectNode =>
                if (
                  writeStrategyNode
                    .hasNonNull("type") || writeStrategyNode
                    .hasNonNull("types")
                ) {
                  writeStrategyNode
                    .path("type") match {
                    case tn: TextNode =>
                      tn.textValue().toUpperCase match {
                        case WriteStrategyType.UPSERT_BY_KEY.value =>
                          val deleteThenInsertNode =
                            TextNode.valueOf(WriteStrategyType.DELETE_THEN_INSERT.value)
                          writeStrategyNode.set[TextNode]("type", deleteThenInsertNode)
                        case _ => // do nothing
                      }
                    case _ => // do nothing
                  }
                  writeStrategyNode
                    .path("types") match {
                    case on: ObjectNode =>
                      on.fields().asScala.toList.foreach { entry =>
                        if (
                          entry.getKey
                            .toUpperCase() == WriteStrategyType.UPSERT_BY_KEY.value
                        ) {
                          on.remove(entry.getKey)
                          on.set[ObjectNode](
                            WriteStrategyType.DELETE_THEN_INSERT.value,
                            entry.getValue
                          )
                        }
                      }
                    case _ => // do nothing
                  }
                }
                writeContainerNode
              case _ => // do nothing
                writeContainerNode
            }
          case n =>
            n
        }
    }

    trait ModeMigrator extends YamlMigratorInterface {
      protected val migrateModeMetadataNode: List[JsonNode => JsonNode] = {
        List[JsonNode => JsonNode] {
          case modeContainerNode: ObjectNode =>
            if (!modeContainerNode.path("mode").isMissingNode) {
              modeContainerNode.remove("mode")
              modeContainerNode
            } else modeContainerNode
          case n => n
        }
      }
    }

    trait MetadataMigrator
        extends YamlMigratorInterface
        with WriteStrategyTypesMigrator
        with SinkMigrator
        with WriteMigrator
        with ModeMigrator {

      protected val migrateMetadata: List[JsonNode => JsonNode] = {
        applyMigrationOnP1(_.path("sink"), identity, migrateSink) ++
        applyMigrationOn(identity, migrateWriteStrategyTypesMetadataNode) ++
        applyMigrationOn(identity, migrateWriteMetadataNode) ++
        applyMigrationOn(identity, migrateModeMetadataNode)
      }
    }

    trait TableOrTransformMigrator extends YamlMigratorInterface with MetadataMigrator {
      protected val isForTable: Boolean // if false then it is for transform
      protected val migrateTableOrTransform: List[JsonNode => JsonNode] =
        applyMigrationOn(_.path("metadata"), migrateMetadata) ++ List[JsonNode => JsonNode] {
          case tableOrTransform: ObjectNode =>
            tableOrTransform.remove("flat")
            val writeNode =
              if (isForTable) tableOrTransform.path("metadata").path("write")
              else tableOrTransform.path("write")
            val mergeNode = tableOrTransform.path("merge")
            val keyNode = mergeNode.path("key")
            val timestampNode = mergeNode.path("timestamp")
            // write strategy is created in migrateMetadata and set when dynamic partition overwrite is used only. AutoTaskDesc don't have this declarative feature.
            val existingWriteStrategyNode = tableOrTransform.path("metadata").path("writeStrategy")
            val writeStrategyType = existingWriteStrategyNode.path("type") match {
              case tn: TextNode => tn.textValue()
              case _            => ""
            }
            val existsWriteStrategyTypes = existingWriteStrategyNode.path("types") match {
              case _: ObjectNode => true
              case _             => false
            }
            (mergeNode.isMissingNode(), writeStrategyType) match {
              case (false, t) if t == WriteStrategyType.OVERWRITE_BY_PARTITION.value =>
                throw new RuntimeException(
                  "Cannot define both dynamic partition overwrite and merge in the same table definition"
                )
              case (true, t) if t == WriteStrategyType.OVERWRITE_BY_PARTITION.value =>
              // do nothing since we already have the final writeStrategy and don't have any merge attribute
              case (false, _) =>
                // compute write strategy based on merge attribute
                val writeStrategyNode = if (isForTable) {
                  if (!tableOrTransform.has("metadata")) {
                    tableOrTransform.set("metadata", mapper.createObjectNode())
                  }
                  existingWriteStrategyNode match {
                    case on: ObjectNode =>
                      // merge only if not already set
                      mergeNode.fieldNames().asScala.foreach { f =>
                        if (on.has(f)) {
                          logger.warn(s"writeStrategy already defined attribute $f. Skipping it.")
                        } else {
                          on.set[ObjectNode](f, mergeNode.get(f))
                        }
                      }
                    case _ =>
                      tableOrTransform
                        .path("metadata")
                        .asInstanceOf[ObjectNode]
                        .set[ObjectNode]("writeStrategy", mergeNode)
                  }
                  tableOrTransform.path("metadata").path("writeStrategy")
                } else {
                  // Before V1, we didn't accept / handle any write strategy for transform so we don't need to consider its existence
                  tableOrTransform.set("writeStrategy", mergeNode)
                  tableOrTransform.path("writeStrategy")
                }
                tableOrTransform.remove("merge")
                if (!existsWriteStrategyTypes) {
                  // only set type if types is not defined.
                  val writeStrategyType: WriteStrategyType =
                    (keyNode.isMissingNode, timestampNode.isMissingNode) match {
                      case (false, false) => WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP
                      case (false, true)  => WriteStrategyType.DELETE_THEN_INSERT
                      case _              =>
                        // if timestamp is defined in merge but not key, doesn't have any effects but still add timestamp into writeStrategy. Maybe we should raise an exception
                        if (writeNode.asText("").toUpperCase == "OVERWRITE") {
                          WriteStrategyType.OVERWRITE
                        } else {
                          WriteStrategyType.APPEND
                        }
                    }
                  writeStrategyNode
                    .asInstanceOf[ObjectNode]
                    .set[ObjectNode]("type", TextNode.valueOf(writeStrategyType.value))
                }
              case _ =>
              // there is no merge node, write strategy has been computed during metadata migration.
            }
            (if (isForTable) tableOrTransform.path("metadata") else tableOrTransform) match {
              case on: ObjectNode => on.remove("write")
              case _              => // do nothing
            }
            tableOrTransform
          case n =>
            n
        }
    }

    trait ExtractTableOrTransformMigrator extends YamlMigratorInterface with MetadataMigrator {
      protected def migrateExtractTable(
        domainFileName: String
      ): List[JsonNode => JsonNode] =
        List[JsonNode => JsonNode] {
          case tableOrTransform: ObjectNode =>
            tableOrTransform.fields().asScala.toList.foreach { field =>
              field.getKey match {
                case "name"    => // do nothing, it will be set as domain's template name
                case "pattern" => // do nothing, just required in table's schema. Overwrite with .*.
                case "attributes" =>
                  field.getValue match {
                    case an: ArrayNode =>
                      an.asScala.foreach {
                        case on: ObjectNode =>
                          on.fields().asScala.toList.foreach { attributesField =>
                            attributesField.getKey match {
                              case "trim" => // keep it as is
                              case _      => on.remove(attributesField.getKey)
                            }
                          }
                          on.set[ObjectNode]("name", TextNode.valueOf("*"))
                        case _ => // do nothing, we expect to have an object node for attributes
                          logger.error("attributes element is not an object node")
                      }
                    case _ => // do nothing, we expect to have an object node for attributes
                      logger.error("attributes is not an array node")
                  }
                case _ => tableOrTransform.remove(field.getKey)
              }
              tableOrTransform.set[ObjectNode](
                "name",
                TextNode.valueOf(domainFileName)
              )
              tableOrTransform.set[ObjectNode](
                "pattern",
                TextNode.valueOf(".*")
              )
            }
            tableOrTransform
          case n =>
            n
        }
    }

    trait SinkMigrator extends YamlMigratorInterface {
      protected val migrateSink: List[JsonNode => JsonNode => JsonNode] = List(
        _ => removeField("type"),
        {
          case parentSinkNode: ObjectNode => {
            case sinkNode: ObjectNode =>
              Option(sinkNode.get("partition")).foreach {
                case partition: ObjectNode if partition.has("attributes") =>
                  sinkNode.set("partition", partition.path("attributes"))
                case _: ArrayNode =>
                // do nothing
                case _ =>
                  // Invalid partition node
                  logger.warn("Invalid partition node")
                  sinkNode.remove("partition")
              }
              Option(sinkNode.get("timestamp")).foreach {
                case timestampNode: ValueNode =>
                  if (sinkNode.has("partition")) {
                    logger.warn("Partition has been superseded by timestamp in metadata.")
                  }
                  val partitions = mapper.createArrayNode()
                  partitions.add(timestampNode)
                  sinkNode.set[ObjectNode]("partition", partitions)
                case _ =>
              }
              sinkNode.remove("timestamp")
              if (
                !(parentSinkNode
                  .path("writeStrategy")
                  .has("type") || parentSinkNode.path("writeStrategy").has("types"))
              ) {
                // we do nothing since write strategy node already exists
                Option(sinkNode.get("dynamicPartitionOverwrite"))
                  .flatMap {
                    case bn: BooleanNode if bn.booleanValue() =>
                      val objectNode = mapper
                        .createObjectNode()
                        .set[ObjectNode](
                          "type",
                          TextNode.valueOf(WriteStrategyType.OVERWRITE_BY_PARTITION.value)
                        )
                      Some(parentSinkNode.set[ObjectNode]("writeStrategy", objectNode))
                    case _ => None
                  }
              }
              sinkNode.remove("dynamicPartitionOverwrite")
              sinkNode
            case n => n
          }
          case _ => identity
        }
      )
    }

    trait TaskMigrator
        extends YamlMigratorInterface
        with SinkMigrator
        with TableOrTransformMigrator {
      override protected val isForTable: Boolean = false

      protected val migrateTask: List[JsonNode => JsonNode] = List(
        renameField("attributesDesc", "attributes"),
        renameField("dataset", "table"),
        removeField("engine"),
        removeField("sqlEngine")
      ) ++ applyMigrationOnP1(_.path("sink"), identity, migrateSink) ++ applyMigrationOn(
        identity,
        migrateWriteMetadataNode
      ) ++ applyMigration(
        migrateTableOrTransform
      )
    }

    trait YamlMigratorInterfaceV1 extends YamlMigratorInterface {

      override val targetVersion: Int = 1
      override def canMigrate(node: JsonNode): Boolean = getVersion(node).isEmpty
    }
    object ExtractConfig extends YamlMigratorInterfaceV1 {
      override val changes: List[JsonNode => JsonNode] = List(
        wrapToContainer("extract"),
        setVersion(targetVersion),
        renameField("extract.globalJdbcSchema", "default")
      )
    }

    object RefsConfig extends YamlMigratorInterfaceV1 {
      override val changes: List[JsonNode => JsonNode] = List(
        wrapToContainer("refs"),
        setVersion(targetVersion)
      )
    }

    object ApplicationConfig extends YamlMigratorInterfaceV1 {
      override val changes: List[JsonNode => JsonNode] = List(
        wrapToContainer("application"),
        setVersion(targetVersion)
      )
    }

    object ExternalConfig extends YamlMigratorInterfaceV1 {
      override val changes: List[JsonNode => JsonNode] = List(
        wrapToContainer("external"),
        setVersion(targetVersion)
      )
    }

    object TaskConfig extends YamlMigratorInterfaceV1 with TaskMigrator {
      override val changes: List[JsonNode => JsonNode] = List(
        wrapToContainer("task"),
        setVersion(targetVersion)
      ) ++ applyMigrationOn(_.path("task"), migrateTask)
    }

    object TableConfig extends YamlMigratorInterfaceV1 with TableOrTransformMigrator {
      override protected val isForTable: Boolean = true

      override val changes: List[JsonNode => JsonNode] = List(
        setVersion(targetVersion)
      ) ++ applyMigrationOn(_.path("table"), migrateTableOrTransform)
    }

    class TableForExtractConfig(domainFileNameWoExt: String)
        extends YamlMigratorInterfaceV1
        with ExtractTableOrTransformMigrator {
      override protected def changes: List[
        JsonNode => JsonNode
      ] = applyMigrationOn(
        _.path("tables"),
        migrateExtractTable(domainFileNameWoExt) ++ List(
          keepFirst(_.path("attributes"))
        )
      ) ++ List(
        keepFirst(
          _.path("tables")
        )
      )
    }

    object LoadConfig
        extends YamlMigratorInterfaceV1
        with TableOrTransformMigrator
        with ExtractTableOrTransformMigrator {
      override protected val isForTable: Boolean = true
      override val changes: List[JsonNode => JsonNode] =
        List(wrapToContainer("load"), setVersion(targetVersion)) ++ applyMigrationOn(
          _.path("load").path("metadata"),
          migrateMetadata
        ) ++ applyMigrationOn(_.path("load"), List(removeField("tables"))) ++ List(
          removeField("load.metadata.write")
        )
    }

    object DagConfig extends YamlMigratorInterfaceV1 {
      override val changes: List[JsonNode => JsonNode] = List(
        wrapToContainer("dag"),
        setVersion(targetVersion)
      )
    }

    object EnvConfig extends YamlMigratorInterfaceV1 {
      override val changes: List[JsonNode => JsonNode] = List(
        wrapToContainer("env"),
        setVersion(targetVersion)
      )
    }

    object TypesConfig extends YamlMigratorInterfaceV1 {
      override val changes: List[JsonNode => JsonNode] = List(
        wrapToContainer("types"),
        setVersion(targetVersion)
      )
    }

    object TransformConfig extends YamlMigratorInterfaceV1 with TaskMigrator {
      override val changes: List[JsonNode => JsonNode] = List(
        wrapToContainer("transform"),
        setVersion(targetVersion)
      ) ++ applyMigrationOn(_.path("transform").path("tasks"), migrateTask)
    }
  }

  object ScalaClass {

    trait YamlMigratorInterfaceScalaClass extends YamlMigratorInterface {

      override val targetVersion: Int = latestSchemaVersion

      override def canMigrate(node: JsonNode): Boolean =
        getVersion(node).contains(latestSchemaVersion)
    }
    object ExtractConfig extends YamlMigratorInterfaceScalaClass {

      override val changes: List[JsonNode => JsonNode] = List(
        transformNode(
          "extract.jdbcSchemas.[*].tables.[*].columns.[*]",
          {
            case columnNode: TextNode =>
              val tableColumn = mapper.createObjectNode()
              tableColumn.set("name", columnNode)
            case e => e
          }
        )
      )
    }
  }
}
