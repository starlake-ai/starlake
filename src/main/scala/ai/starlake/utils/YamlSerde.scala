package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.exceptions.SchemaValidationException
import ai.starlake.extract.JDBCSchemas
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.{
  AutoJobDesc,
  AutoTaskDesc,
  DagDesc,
  DagGenerationConfig,
  Domain,
  EnvDesc,
  ExternalDatabase,
  ExternalDesc,
  IamPolicyTags,
  LoadDesc,
  RefDesc,
  Schema => ModelSchema,
  TableDesc,
  TablesDesc,
  TaskDesc,
  TransformDesc,
  Type,
  TypesDesc,
  WriteStrategyType
}
import better.files.File
import com.fasterxml.jackson.databind.node.{
  ArrayNode,
  BooleanNode,
  MissingNode,
  ObjectNode,
  TextNode
}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.networknt.schema.{
  ApplyDefaultsStrategy,
  JsonSchemaFactory,
  PathType,
  SchemaValidatorsConfig
}
import com.networknt.schema.SpecVersion.VersionFlag
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.util.Locale
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

object YamlSerde extends LazyLogging {
  val mapper: ObjectMapper = Utils.newYamlMapper()

  def serialize[T](entity: T): String = mapper.writeValueAsString(entity)

  /** wrap entity to a container if possible.
    */
  private def wrapEntityToDesc[T](entity: T) = {
    entity match {
      case e: AutoJobDesc  => TransformDesc(latestSchemaVersion, e)
      case e: AutoTaskDesc => TaskDesc(latestSchemaVersion, e)
      case e: Domain       => LoadDesc(latestSchemaVersion, e)
      case e: ModelSchema  => TableDesc(latestSchemaVersion, e)
      case _               => entity
    }
  }

  def serializeToFile[T](targetFile: File, entity: T): Unit = {
    mapper.writeValue(targetFile.toJava, wrapEntityToDesc(entity))
  }

  def serializeToPath[T](targetPath: Path, entity: T)(implicit storage: StorageHandler): Unit = {
    storage.write(serialize(wrapEntityToDesc(entity)), targetPath)
  }

  def deserializeIamPolicyTags(content: String): IamPolicyTags = {
    val rootNode = mapper.readTree(content)
    mapper.treeToValue(rootNode, classOf[IamPolicyTags])
  }

  def toMap(job: AutoJobDesc)(implicit settings: Settings): Map[String, Any] = {
    val jobWriter = mapper
      .writer()
      .withAttribute(classOf[Settings], settings)
    val jsonContent = jobWriter.writeValueAsString(job)
    // val jobReader = mapper.reader().withAttribute(classOf[Settings], settings)
    mapper.readValue(jsonContent, classOf[Map[String, Any]])
  }

  private def forceLocaleIn[T](locale: Locale)(func: => T) = {
    val previousDefault = Locale.getDefault
    Locale.setDefault(locale)
    try {
      func
    } finally {
      Locale.setDefault(previousDefault)
    }
  }

  private def adaptSchemaV7ToStrictV201909(node: JsonNode): JsonNode = {
    def adaptIt(currentNodeName: String, node: JsonNode): (String, JsonNode) = {
      node match {
        case on: ObjectNode =>
          val newObjectNode = mapper.createObjectNode()
          val objectType = Option(on.get("type"))
            .flatMap {
              case t: TextNode => Some(t.asText())
              case _           => None
            }
            .getOrElse("")
          if (
            "object".equalsIgnoreCase(objectType) && !on.has(
              "additionalProperties"
            ) && !currentNodeName.endsWith("Base")
          ) {
            newObjectNode.set[JsonNode]("unevaluatedProperties", BooleanNode.FALSE)
          }
          node.fields().asScala.foreach { kv =>
            val (newKey, newValue) = adaptIt(kv.getKey, kv.getValue)
            newObjectNode.set[JsonNode](newKey, newValue)
          }
          val newNodeName = if (currentNodeName == "definitions") "$defs" else currentNodeName
          newNodeName -> newObjectNode
        case an: ArrayNode =>
          val newArrayNode = mapper
            .createArrayNode()
          newArrayNode
            .addAll(
              an.asScala
                .map { currentNode =>
                  val (_, newValue) = adaptIt(currentNodeName, currentNode)
                  newValue
                }
                .toList
                .asJava
            )
          currentNodeName -> newArrayNode
        case _: TextNode if currentNodeName == "$schema" =>
          currentNodeName -> new TextNode(VersionFlag.V201909.getId)
        case tn: TextNode if currentNodeName == "$ref" =>
          currentNodeName -> new TextNode(tn.asText().replaceFirst("^#/definitions/", "#/\\$defs/"))
        case _ => currentNodeName -> node
      }
    }
    val (_, newSchema) = adaptIt("", node)
    newSchema
  }

  /** Validate and enrich given config with default values defined in schema.
    * @throws SchemaValidationException
    *   If not valid
    */
  @throws[SchemaValidationException]
  def validateConfigFile(subPath: String, content: String, inputFilename: String): JsonNode = {
    val rootNode: JsonNode = mapper.readTree(content)
    if (!rootNode.hasNonNull(subPath)) {
      throw new RuntimeException(
        s"No '$subPath' attribute found in $inputFilename. Please check your config and define it under '$subPath' attribute."
      )
    }
    val validationResult =
      forceLocaleIn(Locale.ROOT) { // Use root instead of ENGLISH otherwise it fallbacks to local language if it exists. ROOT messages are in ENGLISH.
        val factory = JsonSchemaFactory.getInstance(VersionFlag.V201909)
        val config = new SchemaValidatorsConfig()
        config.setPathType(PathType.JSON_PATH)
        config.setFormatAssertionsEnabled(true)
        config.setJavaSemantics(true)
        config.setApplyDefaultsStrategy(new ApplyDefaultsStrategy(true, true, true))
        val starlakeSchema = adaptSchemaV7ToStrictV201909(
          mapper.readTree(getClass.getResourceAsStream("/starlake.schema.json"))
        )
        val schema = factory.getSchema(starlakeSchema, config)
        schema.walk(
          rootNode,
          true
        )
      }
    val validationMessages = validationResult.getValidationMessages.asScala.toList

    if (validationMessages.nonEmpty) {
      val formattedErrors = validationMessages
        .map(error => error.getInstanceLocation.toString -> error.toString)
        .groupBy { case (location, _) =>
          location
        }
        .mapValues(_.map { case (_, errorMessage) =>
          errorMessage
        }.mkString("\n     - ", "\n     - ", ""))
        .values
        .toList
        .sorted
        .mkString("")
      throw new SchemaValidationException(
        s"Invalid content for $inputFilename:$formattedErrors"
      )
    }
    rootNode
  }

  def deserializeYamlExtractConfig(content: String, inputFilename: String): JDBCSchemas = {
    val extractSubPath = "extract"
    val extractNode =
      validateConfigFile(extractSubPath, content, inputFilename).path(extractSubPath)
    extractNode match {
      case objectNode: ObjectNode =>
        val globalJdbcSchemaNode = objectNode.path("globalJdbcSchema")
        if (!globalJdbcSchemaNode.isMissingNode) {
          YamlSerde.renameField(objectNode, "globalJdbcSchema", "default")
          logger.warn(
            "'globalJdbcSchema' has been renamed to 'default'"
          )
        }
        transformNode(objectNode.path("jdbcSchemas")) { case jdbcSchemaArrayNode: ArrayNode =>
          jdbcSchemaArrayNode.asScala.map(n =>
            transformNode(n.path("tables")) { case tablesArrayNode: ArrayNode =>
              tablesArrayNode.asScala.map(n =>
                transformNode(n.path("columns")) { case columnsArrayNode: ArrayNode =>
                  val updatedColumnsArrayNode = columnsArrayNode.asScala.map {
                    case columnNode: TextNode =>
                      val tableColumn = mapper.createObjectNode()
                      tableColumn.set("name", columnNode)
                    case e => e
                  }
                  columnsArrayNode.removeAll()
                  columnsArrayNode.addAll(updatedColumnsArrayNode.asJavaCollection)
                }
              )
            }
          )
        }
      case _ =>
    }
    val jdbcSchemas = mapper.treeToValue(extractNode, classOf[JDBCSchemas])
    jdbcSchemas.propageGlobalJdbcSchemas()
  }

  def transformNode(node: JsonNode)(partialFunction: PartialFunction[JsonNode, Any]): Any = {
    if (partialFunction.isDefinedAt(node)) {
      partialFunction(node)
    } else {
      node match {
        case node: MissingNode => node
        case _ =>
          throw new RuntimeException(
            s"jdbcSchemas is not expected to be a ${node.getClass.getSimpleName}"
          )
      }
    }
  }

  def deserializeYamlRefs(content: String, path: String): RefDesc = {
    val refsSubPath = "refs"
    val refsNode = validateConfigFile(refsSubPath, content, path)
    mapper.treeToValue(refsNode, classOf[RefDesc])
  }

  def deserializeYamlApplication(content: String, path: String): JsonNode = {
    val refsSubPath = "application"
    validateConfigFile(refsSubPath, content, path)
  }

  def deserializeYamlExternal(content: String, path: String): List[ExternalDatabase] = {
    val refsSubPath = "external"
    val externalNode = validateConfigFile(refsSubPath, content, path)
    mapper
      .treeToValue(externalNode, classOf[ExternalDesc])
      .external
      .projects
      .getOrElse(Nil)
  }

  def deserializeYamlTaskAsJson(content: String, path: String): ObjectNode = {
    val refsSubPath = "task"
    validateConfigFile(refsSubPath, content, path).path("task") match {
      case oNode: ObjectNode => oNode
      case _ =>
        throw new RuntimeException("Should never happen since it has been validated")
    }
  }

  def deserializeYamlTables(content: String, path: String): TablesDesc = {
    Try {
      val rootNode = mapper.readTree(content).asInstanceOf[ObjectNode]
      val tableSubPath = "table"
      val tableListSubPath = "tables"
      YamlSerde.renameField(rootNode, "schema", tableSubPath)
      YamlSerde.renameField(rootNode, "schemas", tableListSubPath)
      if (rootNode.hasNonNull(tableListSubPath)) {
        val tablesNode = validateConfigFile(tableListSubPath, content, path)
        tablesNode.path(tableListSubPath).asInstanceOf[ArrayNode].asScala.foreach { tableNode =>
          mergeToStrategyForTable(tablesNode.asInstanceOf[ObjectNode])
        }
        mapper.treeToValue(tablesNode, classOf[TablesDesc])
      } else {
        // fallback to table since this is how we should define tables in starlake
        val tableNode = validateConfigFile(tableSubPath, content, path)
        mergeToStrategy(tableNode.path(tableSubPath).asInstanceOf[ObjectNode])
        val ref = mapper.treeToValue(tableNode, classOf[TableDesc])
        TablesDesc(ref.version, List(ref.table))
      }
    } match {
      case Success(value) => value
      case Failure(exception) =>
        exception.printStackTrace()
        throw new Exception(s"Invalid Schema file: $path(${exception.getMessage})", exception)
    }
  }

  def deserializeYamlLoadConfig(content: String, path: String): Try[Domain] = {
    Try {
      val loadSubPath = "load"
      val domainNode = validateConfigFile(loadSubPath, content, path)
      val loadNode = domainNode.path(loadSubPath)
      mergeToStrategy(loadNode.asInstanceOf[ObjectNode])
      mergeToStrategyForMetadata(loadNode.path("metadata"))
      mapper.treeToValue(domainNode, classOf[LoadDesc])
    } match {
      case Success(value) => Success(value.load)
      case Failure(exception) =>
        logger.error(s"Invalid domain file: $path(${exception.getMessage})")
        Failure(exception)
    }
  }

  def deserializeYamlTypes(content: String, path: String): List[Type] = {
    val refsSubPath = "types"
    val refsNode = validateConfigFile(refsSubPath, content, path)
    mapper.treeToValue(refsNode, classOf[TypesDesc]).types
  }

  def deserializeYamlDagConfig(content: String, path: String): Try[DagGenerationConfig] = {
    Try {
      val dagSubPath = "dag"
      val dagNode = validateConfigFile(dagSubPath, content, path)
      mapper.treeToValue(dagNode, classOf[DagDesc])
    } match {
      case Success(value) => Success(value.dag)
      case Failure(exception) =>
        logger.error(s"Invalid dag file: $path(${exception.getMessage})")
        Failure(exception)
    }
  }

  def deserializeYamlEnvConfig(content: String, path: String): EnvDesc = {
    val envSubPath = "env"
    val dagNode = validateConfigFile(envSubPath, content, path)
    mapper.treeToValue(dagNode, classOf[EnvDesc])
  }

  // Used by starlake-api
  def deserializeTask(content: String): AutoTaskDesc = {
    mapper.treeToValue(deserializeYamlTaskAsJson(content, "no-file-path"), classOf[AutoTaskDesc])
  }

  def deserializeTaskNode(taskNode: ObjectNode): AutoTaskDesc =
    mapper.treeToValue(taskNode, classOf[AutoTaskDesc])

  def upgradeTaskNode(taskNode: ObjectNode): Unit = {
    YamlSerde.renameField(taskNode, "dataset", "table")
    YamlSerde.renameField(taskNode, "sqlEngine", "engine")
    taskNode.path("sink") match {
      case node if node.isMissingNode => // do nothing
      case sinkNode =>
        sinkNode.path("type") match {
          case node if node.isMissingNode => // do nothing
          case node =>
            val textNode = node.asInstanceOf[TextNode]
            val sinkType = textNode.textValue().replaceAll("\"", "").toUpperCase()
            val parent = sinkNode.asInstanceOf[ObjectNode]
            if (sinkType == "DATABRICKS" || sinkType == "HIVE")
              parent.replace("type", new TextNode("FS"))
            else if (sinkType == "BIGQUERY")
              parent.replace("type", new TextNode("BQ"))
            else if (sinkType == "SF")
              parent.replace("type", new TextNode("SNOWFLAKE"))
        }
    }
    val strategyNode = taskNode.path("writeStrategy")
    if (strategyNode.isMissingNode()) {
      val writeNode = taskNode.path("write")
      if (!writeNode.isMissingNode()) {
        val writeType = writeNode.asInstanceOf[TextNode].textValue().toUpperCase()
        val strategyNode =
          if (writeType == "OVERWRITE")
            mapper.readTree("{type: OVERWRITE}")
          else
            mapper.readTree("{type: APPEND}")
        taskNode.set("writeStrategy", strategyNode)
      }
    }
  }

  def deserializeYamlTransform(content: String, path: String): Try[AutoJobDesc] = {
    Try {
      val transformSubPath = "transform"
      val transformNode = validateConfigFile(transformSubPath, content, path)
      transformNode.path(transformSubPath).path("tasks") match {
        case tasksNode: ArrayNode =>
          tasksNode.asScala.foreach {
            case oNode: ObjectNode => upgradeTaskNode(oNode)
            case _ =>
              throw new RuntimeException("Task definition is expected to be an object")
          }
        case _: MissingNode =>
        // do nothing
        case _ =>
          throw new RuntimeException("Tasks is expected to be an object")
      }
      mapper.treeToValue(transformNode, classOf[TransformDesc]).transform
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        logger.error(s"Invalid transform file: $path(${exception.getMessage})")
        Failure(exception)
    }
  }

  private def mergeToStrategy(rootNode: ObjectNode) = {
    val tableNode = rootNode.path("table")
    val tablesNode = rootNode.path("tables")
    if (!tableNode.isMissingNode)
      mergeToStrategyForTable(tableNode.asInstanceOf[ObjectNode])
    if (!tablesNode.isMissingNode) {
      val tablesArray = tablesNode.asInstanceOf[ArrayNode].asScala
      tablesArray.foreach { tableNode =>
        mergeToStrategyForTable(tableNode.asInstanceOf[ObjectNode])
      }
    }
  }

  private def mergeToStrategyForTable(tableNode: ObjectNode) = {
    val strategyNode = tableNode.path("writeStrategy")
    val mergeNode = tableNode.path("merge")
    val keyNode = mergeNode.path("key")
    val timestampNode = mergeNode.path("timestamp")
    val scd2Node = mergeNode.path("scd2")
    val metadataNode = tableNode.path("metadata")
    mergeToStrategyForMetadata(metadataNode)
    (mergeNode.isMissingNode(), strategyNode.isMissingNode()) match {
      case (false, false) =>
        throw new RuntimeException(
          "Cannot define both strategy and merge in the same table definition"
        )
      case (true, false) =>
        val strategyName = strategyNode.path("type").asText().toUpperCase()

        if (!metadataNode.isMissingNode) {
          if (strategyName == "OVERWRITE") {
            metadataNode.asInstanceOf[ObjectNode].set("write", new TextNode("OVERWRITE"))
          } else {
            metadataNode.asInstanceOf[ObjectNode].set("write", new TextNode("APPEND"))
          }
        } else {
          val metadataNodeNew = mapper.readTree("{}")
          tableNode.set("metadata", metadataNodeNew)
        }
        tableNode.path("metadata").asInstanceOf[ObjectNode].set("writeStrategy", strategyNode)
        tableNode.remove("writeStrategy")
      case (false, true) =>
        metadataNode.asInstanceOf[ObjectNode].set("writeStrategy", mergeNode)
        tableNode.remove("merge")
        if (!metadataNode.isMissingNode)
          metadataNode.asInstanceOf[ObjectNode].set("write", new TextNode("APPEND"))
        if (!scd2Node.isMissingNode) {
          mergeNode.asInstanceOf[ObjectNode].set("type", new TextNode("SCD2"))
          mergeNode.asInstanceOf[ObjectNode].remove("scd2")
        } else if (!timestampNode.isMissingNode) {
          mergeNode
            .asInstanceOf[ObjectNode]
            .set("type", new TextNode(WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP.value))
        } else if (!keyNode.isMissingNode) {
          mergeNode
            .asInstanceOf[ObjectNode]
            .set("type", new TextNode(WriteStrategyType.UPSERT_BY_KEY.value))
        } else {
          throw new RuntimeException(
            "Cannot define merge without key, timestamp or scd2 in the same table definition"
          )
        }
      case (true, true) =>
        // merge nod and strategy node are both missing we will use metadata.write value
        if (!metadataNode.isMissingNode) {
          if (metadataNode.path("writeStrategy").isMissingNode()) {
            val writeNode = metadataNode.asInstanceOf[ObjectNode].path("write")
            if (!writeNode.isMissingNode()) {
              val writeType = writeNode.asInstanceOf[TextNode].textValue().toUpperCase()
              val strategyNode =
                if (writeType == "OVERWRITE")
                  mapper.readTree("{type: OVERWRITE}")
                else
                  mapper.readTree("{type: APPEND}")
              metadataNode.asInstanceOf[ObjectNode].set("writeStrategy", strategyNode)
            }
          }
        }
    }
  }

  private def mergeToStrategyForMetadata(metadataNode: JsonNode): Unit = {
    if (!metadataNode.isMissingNode()) {
      val sinkNode = metadataNode.path("sink")
      if (!sinkNode.isMissingNode) {
        val partitionNode = sinkNode.path("partition")
        if (!partitionNode.isMissingNode()) {
          val partitionAttributesNode = partitionNode.path("attributes")
          if (!partitionAttributesNode.isMissingNode()) {
            sinkNode.asInstanceOf[ObjectNode].remove("partition")
            sinkNode.asInstanceOf[ObjectNode].set("partition", partitionAttributesNode)
          }
        }
        val dynamicPartitionOverwrite =
          sinkNode.path("dynamicPartitionOverwrite")
        if (
          !dynamicPartitionOverwrite.isMissingNode() &&
          dynamicPartitionOverwrite.asText().toBoolean
        ) {
          sinkNode.asInstanceOf[ObjectNode].remove("dynamicPartitionOverwrite")
          val strategyNode =
            mapper.readTree("{type: OVERWRITE_BY_PARTITION}")
          metadataNode.asInstanceOf[ObjectNode].set("writeStrategy", strategyNode)
        }

        val timestampNode = sinkNode.path("timestamp")
        if (!timestampNode.isMissingNode()) {
          val timestamp = timestampNode.asText()
          sinkNode.asInstanceOf[ObjectNode].remove("timestamp")
          sinkNode.asInstanceOf[ObjectNode].set("partition", mapper.readTree(s"""["$timestamp"]"""))
        }
      }
    }
  }

  def renameField(node: ObjectNode, oldName: String, newName: String): Unit = {
    val oldNode = node.path(oldName)
    val newNode = node.path(newName)
    if ((newNode.isNull || newNode.isMissingNode) && !(oldNode.isNull || oldNode.isMissingNode)) {
      node.set(newName, oldNode)
      node.remove(oldName)
    }
  }

  def deepChangeFieldValues(
    node: JsonNode,
    propertyName: String,
    oldValue: String,
    newValue: String
  ): Any = {
    val nodes = node.findParents(propertyName).asScala
    nodes.foreach { node =>
      val parentNode = node.asInstanceOf[ObjectNode]
      val propertyNode = parentNode.get(propertyName)
      if (propertyNode.asText() == oldValue) {
        parentNode.put(propertyName, newValue)
      }
    }
  }
}
