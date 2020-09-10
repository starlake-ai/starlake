package com.ebiznext.comet.schema.model.atlas

import com.ebiznext.comet.job.atlas.AtlasConfig
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.typesafe.scalalogging.StrictLogging

class AtlasModel(urls: Array[String], basicAuthUsernamePassword: Array[String])
    extends StrictLogging {
  def run(config: AtlasConfig, storage: StorageHandler): Boolean = ???

  /*
      val PROJECT_TYPE = "Project"
      val BUCKET_TYPE = "Bucket"
      val FILE_TYPE = "DataFile"
      val BQTABLE_TYPE = "BQTable"
      val PROCESS_TYPE = "CometProcess"

      val DOMAIN_TYPE = "Domain"
      val SCHEMA_TYPE = "Schema"
      val ATTRIBUTE_TYPE = "Attribute"
      val METADATA_STRUCT = "Metadata"
      val POSITION_STRUCT = "Position"
      val MERGE_OPTIONS_STRUCT = "MergeOptions"

      val QUALIFIED_NAME = "qualifiedName"
      val REFERENCEABLE_ATTRIBUTE_NAME: String = QUALIFIED_NAME
      val CLUSTER_SUFFIX = "@cl1"
      val atlasClientV2 = new AtlasClientV2(urls, basicAuthUsernamePassword)
   */
  /*
  private def toAtlasClassifications(
    classificationNames: Array[String]
  ): java.util.List[AtlasClassification] = {
    import java.util
    import java.util.Arrays.asList
    val ret: util.List[AtlasClassification] = new util.ArrayList[AtlasClassification]
    val classifications: util.List[String] = asList(classificationNames: _*)
    if (CollectionUtils.isNotEmpty(classifications)) {
      import scala.collection.JavaConversions._
      for (classificationName <- classifications) {
        ret.add(new AtlasClassification(classificationName))
      }
    }
    ret
  }

  def getAtlasObjectId(entity: AtlasEntity): AtlasObjectId =
    new AtlasObjectId(entity.getGuid, entity.getTypeName)

  def toAtlasRelatedObjectId(entity: AtlasEntity): AtlasRelatedObjectId =
    new AtlasRelatedObjectId(getAtlasObjectId(entity))

  def toAtlasRelatedObjectId(objectId: AtlasObjectId): AtlasRelatedObjectId =
    new AtlasRelatedObjectId(objectId)

  def toAtlasRelatedObjectIds(
    entities: java.util.Collection[AtlasEntity]
  ): java.util.Collection[AtlasRelatedObjectId] = {
    val ret = new java.util.ArrayList[AtlasRelatedObjectId]
    if (CollectionUtils.isNotEmpty(entities)) {
      import scala.collection.JavaConversions._
      for (entity <- entities) {
        if (entity != null) ret.add(toAtlasRelatedObjectId(entity))
      }
    }
    ret
  }

  @throws[Exception]
  private def getOrCreateInstance(entity: AtlasEntity): AtlasEntity =
    getOrCreateInstance(new AtlasEntity.AtlasEntityWithExtInfo(entity))

  @throws[Exception]
  private def findInstance(entity: AtlasEntity): Try[AtlasEntity] = {
    findInstance(new AtlasEntity.AtlasEntityWithExtInfo(entity))
  }

  @throws[Exception]
  protected def deleteEntity(guid: String) =
    atlasClientV2.deleteEntityByGuid(guid)

  @throws[Exception]
  private def getOrCreateInstance(
    entityWithExtInfo: AtlasEntity.AtlasEntityWithExtInfo
  ): AtlasEntity = {
    val typeName = entityWithExtInfo.getEntity().getTypeName
    val objectName =
      entityWithExtInfo.getEntity().getAttribute(REFERENCEABLE_ATTRIBUTE_NAME).asInstanceOf[String]
    val existingEntity = getEntity(objectName, typeName)
    existingEntity match {
      case Failure(_) =>
        val response = atlasClientV2.createEntity(entityWithExtInfo)
        val entities = response.getEntitiesByOperation(EntityOperation.CREATE)
        if (CollectionUtils.isNotEmpty(entities)) {
          val getByGuidResponse = atlasClientV2.getEntityByGuid(entities.get(0).getGuid)
          val ret = getByGuidResponse.getEntity
          logger.info("Created entity of type [" + ret.getTypeName + "], guid: " + ret.getGuid)
          ret
        } else
          null
      case Success(value) =>
        value
    }
  }

  @throws[Exception]
  private def findInstance(
    entityWithExtInfo: AtlasEntity.AtlasEntityWithExtInfo
  ): Try[AtlasEntity] = {
    val typeName = entityWithExtInfo.getEntity().getTypeName
    val objectName =
      entityWithExtInfo.getEntity().getAttribute(REFERENCEABLE_ATTRIBUTE_NAME).asInstanceOf[String]
    getEntity(objectName, typeName)
  }

  def objectId(guid: String, `type`: String): AtlasObjectId = new AtlasObjectId(guid, `type`)

  def referenceable(
    qualifiedName: Array[String],
    name: String,
    description: String,
    owner: String,
    `type`: String
  ): AtlasEntity = {
    val entity = new AtlasEntity(`type`)
    // set attributes
    entity.setAttribute("name", name)
    entity.setAttribute(REFERENCEABLE_ATTRIBUTE_NAME, qualifiedName.mkString(".") + CLUSTER_SUFFIX)
    entity.setAttribute("description", description)
    entity.setAttribute("owner", owner)
    entity
  }

  def project(name: String, description: String, owner: String): AtlasEntity =
    getOrCreateInstance(referenceable(Array(name), name, description, owner, PROJECT_TYPE))

  def bucket(
    qualifiedName: Array[String],
    name: String,
    description: String,
    owner: String,
    project: AtlasObjectId
  ): AtlasEntity =
    getOrCreateInstance(referenceable(qualifiedName, name, description, owner, BUCKET_TYPE))

  val fileAttrs = Array(
    "name",
    "description",
    "owner",
    "size",
    "rowCount",
    "status",
    "isFile",
    "format"
  )

  def file(
    qualifiedName: Array[String],
    name: String,
    description: String,
    owner: String,
    createTime: Long,
    modifiedTime: Long,
    size: Long,
    rowCount: Long,
    status: String,
    isFile: Boolean,
    format: String,
    bucket: AtlasObjectId
  ): AtlasEntity = {
    val entity = referenceable(qualifiedName, name, description, owner, FILE_TYPE)
    entity.setAttribute("createTime", createTime)
    entity.setAttribute("modifiedTime", modifiedTime)
    entity.setAttribute("size", size)
    entity.setAttribute("rowCount", rowCount)
    entity.setAttribute("status", status)
    entity.setAttribute("isFile", isFile)
    entity.setAttribute("format", format)
    getOrCreateInstance(entity)
    entity
  }

  val metadataAttrs = Array(
    "name",
    "description",
    "owner",
    "mode",
    "format",
    "encoding",
    "multiline",
    "array",
    "withHeader",
    "separator",
    "escape",
    "quote",
    "write",
    "properties"
  )
  private def createMetadata(metadata: Metadata) = {
    val meta = new AtlasStruct(METADATA_STRUCT)
    meta.setAttribute("mode", metadata.getIngestMode().value)
    meta.setAttribute("format", metadata.getFormat().value)
    meta.setAttribute("encoding", metadata.getEncoding())
    meta.setAttribute("multiline", metadata.getMultiline())
    meta.setAttribute("array", metadata.isArray())
    meta.setAttribute("withHeader", metadata.isWithHeader())
    meta.setAttribute("separator", metadata.getSeparator())
    meta.setAttribute("escape", metadata.getEscape())
    meta.setAttribute("quote", metadata.getQuote())
    meta.setAttribute("write", metadata.getWriteMode().value)
    metadata.getIndexSink().foreach(sink => meta.setAttribute("index", sink.value))
    import scala.collection.JavaConversions.mapAsJavaMap
    meta.setAttribute("properties", mapAsJavaMap(metadata.getProperties()))
    meta
  }

  val mergeOptionsAttrs = Array("key", "delete", "timestamp")
  private def createMergeOptions(options: MergeOptions) = {
    val merge = new AtlasStruct(MERGE_OPTIONS_STRUCT)
    merge.setAttribute("key", options.key.toArray)
    options.delete.foreach { del =>
      merge.setAttribute("delete", del)
    }
    options.timestamp.foreach { time =>
      merge.setAttribute("timestamp", time)
    }
    merge
  }

  val domainAttrs = Array(
    "name",
    "description",
    "owner",
    "directory",
    "metadata",
    "extensions",
    "ack"
  )

  def getOrCreateDomain(
    qualifiedName: Array[String],
    name: String,
    description: String,
    owner: String,
    directory: String,
    metadata: Option[Metadata],
    extensions: Option[Array[String]],
    ack: Option[String]
  ): AtlasEntity = {
    val entity = referenceable(qualifiedName, name, description, owner, DOMAIN_TYPE)
    entity.setAttribute("directory", directory)
    metadata.foreach { metadata =>
      val meta = createMetadata(metadata)
      entity.setAttribute("metadata", meta)
    }
    extensions.foreach { extensions =>
      entity.setAttribute("extensions", extensions)
    }
    ack.foreach(ack => entity.setAttribute("ack", ack))

    val result = getOrCreateInstance(entity)
    /*
    val existingEntity = findInstance(entity)
    val result = existingEntity match {
      case Failure(_)              => getOrCreateInstance(entity)
      case Success(existingEntity) =>
        if (isEntityUpdated(existingEntity, entity))) {
        }
        else {
    existingEntity
      }
    }
    val existingDirectory = result.getAttribute("directory").asInstanceOf[String]
    val existingMetadata = Option(result.getAttribute("metadata").asInstanceOf[AtlasStruct])
    val existingExtensions = Option(result.getAttribute("extensions").asInstanceOf[Array[String]])
   */
    result
  }

  def isEntityUpdated(entity1: AtlasEntity, entity2: AtlasEntity): Boolean = {
    false
  }

  val attributeAttrs = Array(
    "name",
    "description",
    "owner",
    "type",
    "array",
    "required",
    "privacy",
    "rename",
    "metricType",
    "first",
    "trim",
    "position",
    "default",
    "schema"
  )
  private def getOrCreateAttribute(
    qualifiedName: Array[String],
    name: String,
    description: String,
    owner: String,
    schema: AtlasObjectId,
    `type`: String,
    array: Boolean,
    required: Boolean,
    privacy: String,
    rename: Option[String],
    metricType: String,
    position: Option[Position],
    default: Option[String],
    tags: Set[String]
  ): AtlasEntity = {
    val entity = referenceable(qualifiedName, name, description, owner, ATTRIBUTE_TYPE)
    entity.setAttribute("name", name)
    entity.setAttribute("description", description)
    entity.setAttribute("owner", owner)
    entity.setAttribute("type", `type`)
    entity.setAttribute("array", array)
    entity.setAttribute("required", required)
    entity.setAttribute("privacy", privacy)
    rename.foreach { rename =>
      entity.setAttribute("rename", rename)
    }
    entity.setAttribute("metricType", metricType)
    position.foreach { position =>
      val pos = new AtlasStruct(POSITION_STRUCT)
      pos.setAttribute("first", position.first)
      pos.setAttribute("last", position.last)
      position.trim.foreach(trim => pos.setAttribute("trim", trim))
      entity.setAttribute("position", pos)
    }
    default.foreach { default =>
      entity.setAttribute("default", default)
    }
    //entity.setLabels(labels)
    entity.setRelationshipAttribute("schema", toAtlasRelatedObjectId(schema))
    getOrCreateInstance(entity)
  }

  def getOrCreateSchema(
    qualifiedName: Array[String],
    name: String,
    description: String,
    owner: String,
    domain: AtlasObjectId,
    pattern: String,
    metadata: Option[Metadata],
    merge: Option[MergeOptions],
    attributes: List[Attribute],
    tags: Set[String]
  ): AtlasEntity = {
    val entity = referenceable(qualifiedName, name, description, owner, SCHEMA_TYPE)
    entity.setAttribute("pattern", pattern)
    metadata.foreach(metadata => entity.setAttribute("metadata", createMetadata(metadata)))
    merge.foreach { merge =>
      entity.setAttribute("merge", createMergeOptions(merge))
    }
    //entity.setLabels(tags)
    entity.setRelationshipAttribute("domain", toAtlasRelatedObjectId(domain))
    val sch = getOrCreateInstance(entity)
    val atlasAttrs: List[AtlasEntity] = attributes.map { attribute =>
      getOrCreateAttribute(
        attribute.name +: qualifiedName,
        attribute.name,
        attribute.comment.getOrElse(""),
        owner,
        getAtlasObjectId(sch),
        attribute.`type`,
        attribute.isArray(),
        attribute.required,
        "A", //attribute.getPrivacy().value,
        attribute.rename,
        attribute.getMetricType().value,
        attribute.position,
        attribute.default,
        attribute.tags.getOrElse(Set.empty[String])
      )
    }
    entity
  }

  @throws[AtlasServiceException]
  protected def getObjectId(objectName: String, objectType: String): Try[AtlasObjectId] = {
    val attributes =
      java.util.Collections.singletonMap(REFERENCEABLE_ATTRIBUTE_NAME, objectName + CLUSTER_SUFFIX)
    val entity = Try(atlasClientV2.getEntityByAttribute(objectType, attributes).getEntity)
    entity.map { entity =>
      new AtlasObjectId(entity.getGuid, objectType)
    }
  }

  @throws[AtlasServiceException]
  protected def getEntity(qualifiedName: String, objectType: String): Try[AtlasEntity] = {
    val attributes =
      java.util.Collections.singletonMap(REFERENCEABLE_ATTRIBUTE_NAME, qualifiedName)
    Try(atlasClientV2.getEntityByAttribute(objectType, attributes).getEntity)
  }

  def run(config: AtlasConfig, storage: StorageHandler): Unit = {
    val deleteFirst = config.delete
    val folderFiles =
    config.folder.map { folder =>
      storage.list(new Path(folder), ".yml").map(_.toString)
    } getOrElse (Nil)
    val allFiles = config.files ++ folderFiles
    allFiles.foreach { file =>
      val domainContent = storage.read(new Path(file))
      createAtlasModel(storage, domainContent, deleteFirst)
    }
  }

  private def createAtlasModel(
    storage: StorageHandler,
    domainContent: String,
    deleteFirst: Boolean
  ) = {
    val sch = new SchemaHandler(storage)
    val dom = sch.mapper.readValue(domainContent, classOf[Domain])

    val entity = getObjectId(dom.name, DOMAIN_TYPE)

    if (deleteFirst) entity.foreach(entity => deleteEntity(entity.getGuid))
    val domainEntity = getOrCreateDomain(
      Array(dom.name),
      dom.name,
      dom.comment.getOrElse(""),
      settings.comet.atlas.owner,
      dom.directory,
      dom.metadata,
      dom.extensions.map(_.toArray),
      dom.ack
    )
    dom.schemas.foreach { sch =>
      val entity = getObjectId(sch.name, SCHEMA_TYPE)
      entity.foreach { entity =>
        if (deleteFirst) deleteEntity(entity.getGuid)
        sch.attributes.foreach { attr =>
          val entity = getObjectId(attr.name, ATTRIBUTE_TYPE)
          entity.foreach(entity => if (deleteFirst) deleteEntity(entity.getGuid))
        }
      }
      getOrCreateSchema(
        Array(sch.name, dom.name),
        sch.name,
        sch.comment.getOrElse(""),
        "hayssams-test",
        getAtlasObjectId(domainEntity),
        sch.pattern.toString,
        sch.metadata,
        sch.merge,
        sch.attributes,
        sch.tags.getOrElse(Set())
      )
    }
  }
   */
}
