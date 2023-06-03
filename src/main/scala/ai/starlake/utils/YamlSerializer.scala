package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.extract.JDBCSchemas
import ai.starlake.schema.model.{
  AutoJobDesc,
  AutoTaskDesc,
  DagGenerationConfig,
  Domain,
  IamPolicyTags,
  Schema => ModelSchema,
  SchemaRef,
  SchemaRefs
}
import better.files.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

object YamlSerializer extends LazyLogging {
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  Utils.setMapperProperties(mapper)

  def serialize(domain: Domain): String = mapper.writeValueAsString(domain)

  def serialize(iamPolicyTags: IamPolicyTags): String = mapper.writeValueAsString(iamPolicyTags)

  def deserializeIamPolicyTags(content: String): IamPolicyTags = {
    val rootNode = mapper.readTree(content)
    mapper.treeToValue(rootNode, classOf[IamPolicyTags])
  }

  def serialize(autoJob: AutoJobDesc): String = mapper.writeValueAsString(autoJob)

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
      if (extractNode.isNull() || extractNode.isMissingNode) {
        logger.warn(
          s"Defining a jdbc schema outside a extract node is now deprecated. Please update definition ${inputFilename}"
        )
        rootNode
      } else
        extractNode
    if (
      jdbcNode
        .path("globalJdbcSchema")
        .isMissingNode && !jdbcNode.path("globalJdbcSchema").path("tables").isMissingNode
    ) {
      logger.warn(
        "tables defined in globalJdbcSchema are ignored. Please define them in jdbcSchemas"
      )
    }
    val jdbcSchemas = mapper.treeToValue(jdbcNode, classOf[JDBCSchemas])
    jdbcSchemas.propageGlobalJdbcSchemas()
  }

  def serializeToFile(targetFile: File, autoJobDesc: AutoJobDesc): Unit = {
    case class Transform(transform: AutoJobDesc)
    mapper.writeValue(targetFile.toJava, Transform(autoJobDesc))
  }

  def serializeToFile(targetFile: File, domain: Domain): Unit = {
    case class Load(load: Domain)
    mapper.writeValue(targetFile.toJava, Load(domain))
  }

  def serializeToFile(targetFile: File, iamPolicyTags: IamPolicyTags): Unit = {
    mapper.writeValue(targetFile.toJava, iamPolicyTags)
  }

  def serializeToFile(targetFile: File, schemaRef: SchemaRef): Unit = {
    mapper.writeValue(targetFile.toJava, schemaRef)
  }

  def deserializeSchemaRefs(content: String, path: String): SchemaRefs = {
    Try {
      val rootNode = mapper.readTree(content).asInstanceOf[ObjectNode]
      YamlSerializer.renameField(rootNode, "schema", "table")
      val tableNode = rootNode.path("table")
      if (tableNode.isNull || tableNode.isMissingNode) {
        YamlSerializer.renameField(rootNode, "schemas", "tables")
        mapper.treeToValue(rootNode, classOf[SchemaRefs])
      } else {
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
        if (loadNode.isNull() || loadNode.isMissingNode) {
          rootNode.asInstanceOf[ObjectNode]
        } else
          loadNode.asInstanceOf[ObjectNode]
      renameField(domainNode, "schemas", "tables")
      renameField(domainNode, "schemaRefs", "tableRefs")
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

  def deserializeDagGenerationConfig(content: String, path: String): Try[DagGenerationConfig] = {
    Try {
      val rootNode = mapper.readTree(content)
      val dagNode = rootNode.path("dag")
      if (dagNode.isNull() || dagNode.isMissingNode) {
        throw new RuntimeException(
          s"No 'dag' attribute found in $path. Please define your dag generation config under 'dags' attribute."
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
      if (taskNode.isNull() || taskNode.isMissingNode) {
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
          case node if node.isMissingNode => // do thing
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
        if (transformNode.isNull() || transformNode.isMissingNode) {
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
        logger.error(s"Invalid job file: $path(${exception.getMessage})")
        Failure(exception)
    }
  }

  def renameField(node: ObjectNode, oldName: String, newName: String): Any = {
    val oldNode = node.path(oldName)
    val newNode = node.path(newName)
    if ((newNode.isNull || newNode.isMissingNode) && !(oldNode.isNull || oldNode.isMissingNode)) {
      node.set(newName, oldNode)
      node.remove(oldName)
    }
  }
}
