package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.exceptions.SchemaValidationException
import ai.starlake.extract.{ExtractDesc, JDBCSchemas}
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.{
  AutoJobDesc,
  AutoTaskDesc,
  DagDesc,
  DagGenerationConfig,
  Domain,
  EnvDesc,
  IamPolicyTags,
  LoadDesc,
  RefDesc,
  Schema => ModelSchema,
  TableDesc,
  TablesDesc,
  TaskDesc,
  TransformDesc,
  Type,
  TypesDesc
}
import com.fasterxml.jackson.databind.node.{ArrayNode, BooleanNode, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.networknt.schema.{
  ApplyDefaultsStrategy,
  JsonSchemaFactory,
  PathType,
  SchemaValidatorsConfig,
  ValidationResult
}
import com.networknt.schema.SpecVersion.VersionFlag
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import ImplicitRichPath._

import java.util.Locale
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

object YamlSerde extends LazyLogging {
  val mapper: ObjectMapper = Utils.newYamlMapper()

  def serialize[T](entity: T): String = mapper.writeValueAsString(entity)

  /** wrap entity to a container if possible.
    */
  private def wrapEntityToDesc[T](entity: T) = {
    entity match {
      case e: AutoJobDesc         => TransformDesc(latestSchemaVersion, e)
      case e: AutoTaskDesc        => TaskDesc(latestSchemaVersion, e)
      case e: Domain              => LoadDesc(latestSchemaVersion, e)
      case e: ModelSchema         => TableDesc(latestSchemaVersion, e)
      case e: JDBCSchemas         => ExtractDesc(latestSchemaVersion, e)
      case e: DagGenerationConfig => DagDesc(latestSchemaVersion, e)
      case _                      => entity
    }
  }

  def serializeToPath[T](targetPath: Path, entity: T)(implicit storage: StorageHandler): Unit = {
    // TODO REMOVE
    println(serialize(wrapEntityToDesc(entity)))
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
  def validateConfigFile(
    subPath: String,
    content: String,
    inputFilename: String,
    migrationList: List[YamlMigratorInterface],
    postProcess: Option[YamlMigratorInterface] = None
  ): JsonNode = {
    val rawRootNode: JsonNode = mapper.readTree(content)
    val effectiveRootNode = if (migrationList.exists(_.canMigrate(rawRootNode))) {
      logger.warn(s"Migrating config of $inputFilename on-the-fly")
      migrationList.foldLeft(rawRootNode) { case (node, migrator) =>
        migrator.migrate(node)
      }
    } else {
      rawRootNode
    }
    if (!effectiveRootNode.hasNonNull(subPath)) {
      throw new RuntimeException(
        s"No '$subPath' attribute found in $inputFilename. Please check your config and define it under '$subPath' attribute."
      )
    }
    val validationResult: ValidationResult =
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
          effectiveRootNode,
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
    postProcess
      .map { f =>
        if (f.canMigrate(effectiveRootNode)) {
          f.migrate(effectiveRootNode)
        } else {
          throw new RuntimeException(
            s"Post process hasn't been applied for $inputFilename but was expected to."
          )
        }
      }
      .getOrElse(effectiveRootNode)
  }

  def deserializeYamlExtractConfig(
    content: String,
    inputFilename: String,
    propageDefault: Boolean = true
  ): JDBCSchemas = {
    val extractSubPath = "extract"
    val extractNode =
      validateConfigFile(
        extractSubPath,
        content,
        inputFilename,
        List(YamlMigrator.V1.ExtractConfig),
        Some(YamlMigrator.ScalaClass.ExtractConfig)
      ).path(extractSubPath)
    val jdbcSchemas = mapper.treeToValue(extractNode, classOf[JDBCSchemas])
    if (propageDefault) {
      jdbcSchemas.propagateGlobalJdbcSchemas()
    } else {
      jdbcSchemas
    }
  }
  def deserializeYamlRefs(content: String, path: String): RefDesc = {
    val refsSubPath = "refs"
    val refsNode = validateConfigFile(refsSubPath, content, path, List(YamlMigrator.V1.RefsConfig))
    mapper.treeToValue(refsNode, classOf[RefDesc])
  }

  def deserializeYamlApplication(content: String, path: String): JsonNode = {
    val refsSubPath = "application"
    validateConfigFile(refsSubPath, content, path, List(YamlMigrator.V1.ApplicationConfig))
  }

  def deserializeYamlTables(content: String, path: String): TablesDesc = {
    Try {
      val rootNode = mapper.readTree(content).asInstanceOf[ObjectNode]
      val tableSubPath = "table"
      val tableListSubPath = "tables"
      if (rootNode.hasNonNull(tableListSubPath)) {
        val tablesNode =
          validateConfigFile(tableListSubPath, content, path, List(YamlMigrator.V1.TableConfig))
        mapper.treeToValue(tablesNode, classOf[TablesDesc])
      } else {
        // fallback to table since this is how we should define tables in starlake
        val tableNode =
          validateConfigFile(tableSubPath, content, path, List(YamlMigrator.V1.TableConfig))
        val metadata = tableNode.path("metadata")
        val isJsonArray = if (!metadata.isMissingNode) {
          metadata.path("format").asText().toLowerCase() == "array_json"
        } else
          false
        val ref = mapper.treeToValue(tableNode, classOf[TableDesc])
        val table =
          if (isJsonArray)
            ref.table
              .copy(metadata = ref.table.metadata.map(m => m.copy(array = Some(true))))
          else
            ref.table
        TablesDesc(ref.version, List(table))
      }
    } match {
      case Success(value) => value
      case Failure(exception) =>
        exception.printStackTrace()
        throw new Exception(s"Invalid Schema file: $path(${exception.getMessage})", exception)
    }
  }

  def deserializeYamlLoadConfig(
    content: String,
    path: String,
    isForExtract: Boolean
  ): Try[Domain] = {
    Try {
      val loadSubPath = "load"
      val filePath = new Path(path)
      val domainNode =
        validateConfigFile(
          loadSubPath,
          content,
          path,
          List(
            new YamlMigrator.V1.LoadConfig(
              filePath.fileNameWithoutSlExt,
              isForExtract = isForExtract
            )
          )
        )
      mapper.treeToValue(domainNode, classOf[LoadDesc])
    } match {
      case Success(value) => Success(value.load)
      case Failure(exception) =>
        logger.error(s"Invalid domain file: $path(${exception.getMessage})", exception)
        Failure(exception)
    }
  }

  def deserializeYamlTypes(content: String, path: String): List[Type] = {
    val refsSubPath = "types"
    val refsNode = validateConfigFile(refsSubPath, content, path, List(YamlMigrator.V1.TypesConfig))
    mapper.treeToValue(refsNode, classOf[TypesDesc]).types
  }

  def deserializeYamlDagConfig(content: String, path: String): Try[DagGenerationConfig] = {
    Try {
      val dagSubPath = "dag"
      val dagNode = validateConfigFile(dagSubPath, content, path, List(YamlMigrator.V1.DagConfig))
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
    val dagNode = validateConfigFile(envSubPath, content, path, List(YamlMigrator.V1.EnvConfig))
    mapper.treeToValue(dagNode, classOf[EnvDesc])
  }

  // Used by starlake-api
  def deserializeYamlTask(content: String, path: String): AutoTaskDesc = {
    val refsSubPath = "task"
    val taskNode = validateConfigFile(refsSubPath, content, path, List(YamlMigrator.V1.TaskConfig))
      .path("task") match {
      case oNode: ObjectNode => oNode
      case _ =>
        throw new RuntimeException("Should never happen since it has been validated")
    }
    mapper.treeToValue(taskNode, classOf[AutoTaskDesc])
  }

  def deserializeYamlTransform(content: String, path: String): Try[AutoJobDesc] = {
    Try {
      val transformSubPath = "transform"
      val transformNode =
        validateConfigFile(transformSubPath, content, path, List(YamlMigrator.V1.TransformConfig))
      mapper.treeToValue(transformNode, classOf[TransformDesc]).transform
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        logger.error(s"Invalid transform file: $path(${exception.getMessage})")
        Failure(exception)
    }
  }
}
