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
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._

trait YamlMigratorInterface {

  private val versionFieldName = "version"

  val targetVersion: Int

  def canMigrate(node: JsonNode): Boolean

  def migrate(node: JsonNode): JsonNode = changes.foldLeft(node) { case (newNode, changeFunc) =>
    changeFunc(newNode)
  }

  protected def changes: List[JsonNode => JsonNode]

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

  protected def wrapToContainer(containerName: String): JsonNode => JsonNode = {
    case n if n.has(containerName) => n
    case n                         => YamlSerde.mapper.createObjectNode().set(containerName, n)
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
  object V1 {

    trait WriteMigrator extends YamlMigratorInterface {
      protected val migrateWriteMetadataNode: List[JsonNode => JsonNode] =
        List[JsonNode => JsonNode] {
          case writeContainerNode: ObjectNode =>
            if (!writeContainerNode.path("writeStrategy").hasNonNull("type")) {
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
        with SinkMigrator
        with WriteMigrator
        with ModeMigrator {

      protected val migrateMetadata: List[JsonNode => JsonNode] = {
        applyMigrationOnP1(_.path("sink"), identity, migrateSink) ++
        applyMigrationOn(identity, migrateWriteMetadataNode) ++
        applyMigrationOn(identity, migrateModeMetadataNode)
      }
    }

    trait TableOrTransformMigrator extends YamlMigratorInterface with MetadataMigrator {
      protected val isForTable: Boolean // if false then it is for transform
      protected val migrateTableOrTransform: List[JsonNode => JsonNode] =
        applyMigrationOn(_.path("metadata"), migrateMetadata) ++ List[JsonNode => JsonNode] {
          case tableOrTransform: ObjectNode =>
            val writeNode =
              if (isForTable) tableOrTransform.path("metadata").path("write")
              else tableOrTransform.path("write")
            val mergeNode = tableOrTransform.path("merge")
            val keyNode = mergeNode.path("key")
            val timestampNode = mergeNode.path("timestamp")
            // write strategy is created in migrateMetadata and set when dynamic partition overwrite is used only. AutoTaskDesc don't have this declarative feature.
            val writeStrategyType = Option(tableOrTransform.get("metadata"))
              .flatMap(m => Option(m.get("writeStrategy")))
              .flatMap(w => Option(w.get("type")))
              .map {
                case tn: TextNode => tn.textValue()
                case _            => ""
              }
            (mergeNode.isMissingNode(), writeStrategyType) match {
              case (false, Some(t)) if t == WriteStrategyType.OVERWRITE_BY_PARTITION.value =>
                throw new RuntimeException(
                  "Cannot define both dynamic partition overwrite and merge in the same table definition"
                )
              case (true, Some(t)) if t == WriteStrategyType.OVERWRITE_BY_PARTITION.value =>
              // do nothing since we already have the final writeStrategy and don't have any merge attribute
              case (false, _) =>
                // compute write strategy based on merge attribute
                val writeStrategyNode = if (isForTable) {
                  if (!tableOrTransform.has("metadata")) {
                    tableOrTransform.set("metadata", mapper.createObjectNode())
                  }
                  tableOrTransform
                    .path("metadata")
                    .asInstanceOf[ObjectNode]
                    .set("writeStrategy", mergeNode)
                  tableOrTransform.path("metadata").path("writeStrategy")
                } else {
                  tableOrTransform.set("writeStrategy", mergeNode)
                  tableOrTransform.path("writeStrategy")
                }
                tableOrTransform.remove("merge")
                val writeStrategyType: WriteStrategyType =
                  (keyNode.isMissingNode, timestampNode.isMissingNode) match {
                    case (false, false) => WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP
                    case (false, true)  => WriteStrategyType.UPSERT_BY_KEY
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
        setVersion(targetVersion),
        renameField("schema", "table"),
        renameField("schemas", "tables")
      ) ++ applyMigrationOn(_.path("table"), migrateTableOrTransform) ++ applyMigrationOn(
        _.path("tables"),
        migrateTableOrTransform
      )
    }

    object LoadConfig extends YamlMigratorInterfaceV1 with TableOrTransformMigrator {
      override protected val isForTable: Boolean = true
      override val changes: List[JsonNode => JsonNode] =
        List(wrapToContainer("load"), setVersion(targetVersion)) ++ applyMigrationOn(
          _.path("load").path("metadata"),
          migrateMetadata
        ) ++ applyMigrationOn(_.path("load").path("tables"), migrateTableOrTransform) ++ List(
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
