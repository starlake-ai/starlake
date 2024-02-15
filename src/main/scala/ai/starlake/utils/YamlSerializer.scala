package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.extract.JDBCSchemas
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.{
  AutoJobDesc,
  AutoTaskDesc,
  DagGenerationConfig,
  Domain,
  IamPolicyTags,
  LoadDesc,
  Schema => ModelSchema,
  SchemaRef,
  SchemaRefs,
  StrategyType,
  TransformDesc
}
import better.files.{File, UnicodeCharset}
import com.fasterxml.jackson.databind.node.{ArrayNode, MissingNode, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset
import scala.collection.JavaConverters._
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.{Failure, Success, Try}

object YamlSerializer extends LazyLogging {
  val mapper: ObjectMapper = Utils.newYamlMapper()

  implicit val charset: Charset = sun.nio.cs.UTF_8.INSTANCE

  def serialize(domain: Domain): String = mapper.writeValueAsString(domain)

  def serialize(iamPolicyTags: IamPolicyTags): String = mapper.writeValueAsString(iamPolicyTags)

  def deserializeIamPolicyTags(content: String): IamPolicyTags = {
    val rootNode = mapper.readTree(content)
    mapper.treeToValue(rootNode, classOf[IamPolicyTags])
  }

  def serialize(autoJob: AutoJobDesc): String = mapper.writeValueAsString(autoJob)

  def serialize(autoTask: AutoTaskDesc): String = mapper.writeValueAsString(autoTask)

  def serialize(schema: ModelSchema): String = mapper.writeValueAsString(schema)

  def serializeObject(obj: Object): String = mapper.writeValueAsString(obj)

  def toMap(job: AutoJobDesc)(implicit settings: Settings): Map[String, Any] = {
    val jobWriter = mapper
      .writer()
      .withAttribute(classOf[Settings], settings)
    val jsonContent = jobWriter.writeValueAsString(job)
    // val jobReader = mapper.reader().withAttribute(classOf[Settings], settings)
    mapper.readValue(jsonContent, classOf[Map[String, Any]])
  }

  def serialize(jdbcSchemas: JDBCSchemas): String = mapper.writeValueAsString(jdbcSchemas)
  def serialize(schemas: SchemaRefs): String = mapper.writeValueAsString(schemas)

  def deserializeJDBCSchemas(content: String, inputFilename: String): JDBCSchemas = {
    val rootNode = mapper.readTree(content)
    val extractNode = rootNode.path("extract")
    val jdbcNode =
      if (extractNode.isNull || extractNode.isMissingNode) {
        logger.warn(
          s"Defining a jdbc schema outside an extract node is now deprecated. Please update definition $inputFilename"
        )
        rootNode
      } else
        extractNode
    jdbcNode match {
      case objectNode: ObjectNode =>
        val globalJdbcSchemaNode = objectNode.path("globalJdbcSchema")
        if (!globalJdbcSchemaNode.isMissingNode) {
          YamlSerializer.renameField(objectNode, "globalJdbcSchema", "default")
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
    val defaultNode = jdbcNode.path("default")
    if (!defaultNode.isMissingNode) {
      if (!defaultNode.path("tables").isMissingNode) {
        logger.warn(
          "tables defined in default are ignored. Please define them in jdbcSchemas"
        )
      }
      if (!defaultNode.path("exclude").isMissingNode) {
        logger.warn(
          "exclude defined in default is ignored. Please define it in jdbcSchemas"
        )
      }
    }
    val jdbcSchemas = mapper.treeToValue(jdbcNode, classOf[JDBCSchemas])
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

  def serializeToFile(targetFile: File, autoJobDesc: AutoJobDesc): Unit = {
    mapper.writeValue(targetFile.toJava, TransformDesc(autoJobDesc))
  }

  def serializeToFile(targetFile: File, autoTaskDesc: AutoTaskDesc): Unit = {
    case class Task(task: AutoTaskDesc)
    mapper.writeValue(targetFile.toJava, Task(autoTaskDesc))
  }

  def serializeDomain(domain: Domain): String = {
    mapper.writeValueAsString(LoadDesc(domain))
  }

  def serializeToFile(targetFile: File, domain: Domain): Unit = {
    val domainAsString = serializeDomain(domain)
    targetFile.overwrite(domainAsString)(charset = UnicodeCharset(charset))
  }

  def serializeToFile(targetFile: File, schema: ModelSchema): File = {
    targetFile.overwrite(serializeTable(schema))(charset = UnicodeCharset(charset))
  }

  def serializeToPath(targetPath: Path, domain: Domain)(implicit storage: StorageHandler): Unit = {
    val domainAsString = serializeDomain(domain)
    storage.write(domainAsString, targetPath)
  }

  def serializeToPath(targetPath: Path, schema: ModelSchema)(implicit
    storage: StorageHandler
  ): Unit = {
    storage.write(serializeTable(schema), targetPath)
  }

  def serializeTable(schema: ModelSchema): String = {
    case class Table(table: ModelSchema)
    mapper.writeValueAsString(Table(schema))
  }

  def serializeToFile(targetFile: File, iamPolicyTags: IamPolicyTags): Unit = {
    mapper.writeValue(targetFile.toJava, iamPolicyTags)
  }

  def serializeToFile(targetFile: File, schemaRef: SchemaRef): Unit = {
    mapper.writeValue(targetFile.toJava, schemaRef)
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
            .set("type", new TextNode(StrategyType.UPSERT_BY_KEY_AND_TIMESTAMP.value))
        } else if (!keyNode.isMissingNode) {
          mergeNode
            .asInstanceOf[ObjectNode]
            .set("type", new TextNode(StrategyType.UPSERT_BY_KEY.value))
        } else {
          throw new RuntimeException(
            "Cannot define merge without key, timestamp or scd2 in the same table definition"
          )
        }
      case (true, true) =>
        // merge nod and strategy node are both missing we will use metadata.write value
        if (!metadataNode.isMissingNode) {
          if (metadataNode.path("writeStrategy").isMissingNode()) {
            val strategyNode =
              metadataNode.asInstanceOf[ObjectNode].path("write") match {
                case node if node.isMissingNode => // do nothing
                  mapper.readTree("{type: APPEND}")
                case node =>
                  val writeType = node.asInstanceOf[TextNode].textValue().toUpperCase()
                  if (writeType == "OVERWRITE")
                    mapper.readTree("{type: OVERWRITE}")
                  else
                    mapper.readTree("{type: APPEND}")
              }
            metadataNode.asInstanceOf[ObjectNode].set("writeStrategy", strategyNode)
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

  def deserializeSchemaRefs(content: String, path: String): SchemaRefs = {
    Try {
      val rootNode = mapper.readTree(content).asInstanceOf[ObjectNode]
      YamlSerializer.renameField(rootNode, "schema", "table")
      val tableNode = rootNode.path("table")
      if (tableNode.isNull || tableNode.isMissingNode) {
        YamlSerializer.renameField(rootNode, "schemas", "tables")
        val tablesNode = rootNode.path("tables")
        tablesNode.asInstanceOf[ArrayNode].asScala.foreach { tableNode =>
          mergeToStrategyForTable(tableNode.asInstanceOf[ObjectNode])
        }
        val res = mapper.treeToValue(rootNode, classOf[SchemaRefs])
        res
      } else {
        mergeToStrategy(rootNode)
        val ref = mapper.treeToValue(rootNode, classOf[SchemaRef])
        SchemaRefs(List(ref.table))
      }
    } match {
      case Success(value) => value
      case Failure(exception) =>
        exception.printStackTrace()
        throw new Exception(s"Invalid Schema file: $path(${exception.getMessage})", exception)
    }
  }

  def deserializeDomain(content: String, path: String): Try[Domain] = {
    Try {
      val rootNode = mapper.readTree(content)
      val loadNode = rootNode.path("load")
      val domainNode =
        if (loadNode.isNull || loadNode.isMissingNode) {
          rootNode.asInstanceOf[ObjectNode]
        } else
          loadNode.asInstanceOf[ObjectNode]
      renameField(domainNode, "schemas", "tables")
      renameField(domainNode, "schemaRefs", "tableRefs")
      mergeToStrategy(domainNode)
      val metadataNode = domainNode.path("metadata")
      mergeToStrategyForMetadata(metadataNode)

      YamlSerializer.deepChangeFieldValues(rootNode, "type", "None", "Default")
      val domain = mapper.treeToValue(domainNode, classOf[Domain])
      if (domainNode == rootNode)
        logger.warn(
          s"Defining a domain outside a load node is now deprecated. Please update definition fo domain ${domain.name}"
        )

      domain
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        logger.error(s"Invalid domain file: $path(${exception.getMessage})")
        Failure(exception)
    }
  }

  def deserializeSchema(content: String, path: String): Try[ModelSchema] = {
    Try {
      val rootNode = mapper.readTree(content)
      val tableNode = rootNode.path("table").asInstanceOf[ObjectNode]
      val table = mapper.treeToValue(tableNode, classOf[ModelSchema])
      table
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        logger.error(s"Invalid Schema file: $path(${exception.getMessage})")
        Failure(exception)
    }
  }

  def deserializeDagGenerationConfig(content: String, path: String): Try[DagGenerationConfig] = {
    Try {
      val rootNode = mapper.readTree(content)
      val dagNode = rootNode.path("dag")
      if (dagNode.isNull || dagNode.isMissingNode) {
        throw new RuntimeException(
          s"No 'dag' attribute found in $path. Please define your dag generation config under 'dag' attribute."
        )
      }
      mapper.treeToValue(dagNode, classOf[DagGenerationConfig])
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        logger.error(s"Invalid dag file: $path(${exception.getMessage})")
        Failure(exception)
    }
  }

  // Used by starlake-api
  def deserializeTask(content: String): AutoTaskDesc = {
    val rootNode = mapper.readTree(content)
    val taskNode = rootNode.path("task")
    val targetNode =
      if (taskNode.isNull || taskNode.isMissingNode) {
        rootNode.asInstanceOf[ObjectNode]
      } else
        taskNode.asInstanceOf[ObjectNode]
    mapper.treeToValue(targetNode, classOf[AutoTaskDesc])
  }

  def deserializeTaskNode(taskNode: ObjectNode): AutoTaskDesc =
    mapper.treeToValue(taskNode, classOf[AutoTaskDesc])

  def upgradeTaskNode(taskNode: ObjectNode): Unit = {
    YamlSerializer.renameField(taskNode, "dataset", "table")
    YamlSerializer.renameField(taskNode, "sqlEngine", "engine")
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
  }

  def deserializeJob(content: String, path: String): Try[AutoJobDesc] = {
    Try {
      val rootNode = mapper.readTree(content)
      val transformNode = rootNode.path("transform")
      val jobNode =
        if (transformNode.isNull || transformNode.isMissingNode) {
          rootNode.asInstanceOf[ObjectNode]
        } else
          transformNode.asInstanceOf[ObjectNode]
      val job = mapper.treeToValue(jobNode, classOf[AutoJobDesc])
      val tasksNode = jobNode.path("tasks").asInstanceOf[ArrayNode]
      for (i <- 0 until tasksNode.size()) {
        val taskNode = tasksNode.get(i).asInstanceOf[ObjectNode]
        YamlSerializer.renameField(taskNode, "dataset", "table")
      }

      if (jobNode == rootNode)
        logger.warn(
          s"Defining a job outside a load node is now deprecated. Please update definition fo domain ${job.name}"
        )
      job
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        exception.printStackTrace()
        Failure(exception)
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
