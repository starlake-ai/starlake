package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.schema.generator.JDBCSchemas
import ai.starlake.schema.model.{AutoJobDesc, Domain, Schema => ModelSchema, Schemas}
import better.files.File
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

object YamlSerializer extends LazyLogging {
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(Include.NON_EMPTY)

  def serialize(domain: Domain): String = mapper.writeValueAsString(domain)

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

  def deserializeJDBCSchemas(file: File): JDBCSchemas = {
    val rootNode = mapper.readTree(file.newInputStream)
    val extractNode = rootNode.path("extract")
    val jdbcNode =
      if (extractNode.isNull() || extractNode.isMissingNode) {
        logger.warn(
          s"Defining a jdbc schema outside a extract node is now deprecated. Please update definition ${file.pathAsString}"
        )
        rootNode
      } else
        extractNode
    mapper.treeToValue(jdbcNode, classOf[JDBCSchemas])
  }

  def deserializeDomain(file: File): Try[Domain] = {
    deserializeDomain(
      scala.io.Source.fromFile(file.pathAsString).getLines.mkString("\n"),
      file.pathAsString
    )
  }

  def serializeToFile(targetFile: File, domain: Domain): Unit = {
    case class Load(load: Domain)
    mapper.writeValue(targetFile.toJava, Load(domain))
  }

  def serializeToFile(targetFile: File, schema: ModelSchema): Unit = {
    case class Schema(schema: ModelSchema)
    mapper.writeValue(targetFile.toJava, Schema(schema))
  }

  def deserializeSchemas(content: String, path: String): Schemas = {
    Try {
      val rootNode = mapper.readTree(content).asInstanceOf[ObjectNode]
      YamlSerializer.renameField(rootNode, "schemas", "tables")
      mapper.treeToValue(rootNode, classOf[Schemas])

    } match {
      case Success(value) => value
      case Failure(exception) =>
        throw new Exception(s"Invalid Schema file: $path(${exception.getMessage})")
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
        Failure(new Exception(s"Invalid domain file: $path(${exception.getMessage})"))
    }
  }

  def renameField(node: ObjectNode, oldName: String, newName: String) = {
    val oldNode = node.path(oldName)
    val newNode = node.path(newName)
    if ((newNode.isNull || newNode.isMissingNode) && !(oldNode.isNull || oldNode.isMissingNode)) {
      node.set(newName, oldNode)
      node.remove(oldName)
    }
  }
}
