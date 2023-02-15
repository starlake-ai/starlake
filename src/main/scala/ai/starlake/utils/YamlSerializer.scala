package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.extract.JDBCSchemas
import ai.starlake.schema.model.{
  AutoJobDesc,
  AutoTaskDesc,
  Domain,
  IamPolicyTags,
  Schema => ModelSchema,
  Schemas
}
import better.files.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
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

  def serialize(jdbcSchemas: JDBCSchemas): String = mapper.writeValueAsString(jdbcSchemas)
  def serialize(schemas: Schemas): String = mapper.writeValueAsString(schemas)

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
    mapper.treeToValue(jdbcNode, classOf[JDBCSchemas])
  }

  def serializeToFile(targetFile: File, domain: Domain): Unit = {
    case class Load(load: Domain)
    mapper.writeValue(targetFile.toJava, Load(domain))
  }

  def serializeToFile(targetFile: File, iamPolicyTags: IamPolicyTags): Unit = {
    mapper.writeValue(targetFile.toJava, iamPolicyTags)
  }

  def serializeToFile(targetFile: File, schema: ModelSchema): Unit = {
    case class Schema(schema: ModelSchema)
    mapper.writeValue(targetFile.toJava, Schema(schema))
  }

  def deserializeSchemas(content: String, path: String): Schemas = {
    Try {
      val rootNode = mapper.readTree(content).asInstanceOf[ObjectNode]
      YamlSerializer.renameField(rootNode, "schemas", "tables")
      val result = mapper.treeToValue(rootNode, classOf[Schemas])
      result

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
