package ai.starlake.job.sink.bigquery
import ai.starlake.config.Settings
import ai.starlake.schema.model
import ai.starlake.schema.model.{Schema => _, TableInfo => _, _}
import ai.starlake.utils.conversion.BigQueryUtils.sparkToBq
import ai.starlake.utils.{SQLUtils, Utils}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.{PrimaryKey, Schema => BQSchema, TableInfo => BQTableInfo, _}
import com.google.cloud.datacatalog.v1.{
  ListPolicyTagsRequest,
  ListTaxonomiesRequest,
  PolicyTagManagerClient
}
import com.google.cloud.{Identity, Policy, Role, ServiceOptions}
import com.google.iam.v1.{Binding, Policy => IAMPolicy, SetIamPolicyRequest}
import com.google.protobuf.FieldMask
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import java.security.SecureRandom
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Base class for BigQuery jobs
  */
trait BigQueryJobBase extends StrictLogging {

  def settings: Settings
  def cliConfig: BigQueryLoadConfig

  val connectionOptions =
    cliConfig.connection.map(name => settings.comet.connections(name).options)

  def projectId: String = {
    cliConfig.outputDatabase
      .orElse(connectionOptions.flatMap(_.get("database")))
      .orElse(getPropertyOrEnv("SL_DATABASE"))
      .orElse(getPropertyOrEnv("GCP_PROJECT"))
      .orElse(getPropertyOrEnv("GOOGLE_CLOUD_PROJECT"))
      .orElse(scala.Option(ServiceOptions.getDefaultProjectId()))
      .getOrElse(throw new Exception("GCP Project ID must be defined"))
  }

  private def getCredentials()(implicit
    settings: Settings
  ): scala.Option[ServiceAccountCredentials] = {
    val connectionOptions =
      cliConfig.connection.map(name => settings.comet.connections(name).options)
    connectionOptions.flatMap(_.get("serviceAccountKey")).map { gcpSAJsonKey =>
      val gcpSAJsonKeyAsString = if (!gcpSAJsonKey.trim.startsWith("{")) {
        val path = new Path(gcpSAJsonKey)
        if (settings.storageHandler.exists(path)) {
          settings.storageHandler.read(path)
        } else {
          throw new Exception(s"Invalid GCP SA KEY Path: $path")
        }
      } else
        gcpSAJsonKey
      val credentialsStream = new java.io.ByteArrayInputStream(
        gcpSAJsonKeyAsString.getBytes(java.nio.charset.StandardCharsets.UTF_8.name)
      )
      ServiceAccountCredentials.fromStream(credentialsStream)
    }
  }

  // Lazy otherwise tests fail since there is no GCP credentials in test mode
  private lazy val policyTagClient: PolicyTagManagerClient = PolicyTagManagerClient.create()

  def applyRLSAndCLS(forceApply: Boolean = false)(implicit settings: Settings): Try[Unit] = {
    for {
      _ <- applyRLS(forceApply)
      _ <- applyCLS(forceApply)
    } yield ()
  }

  def bigquery()(implicit settings: Settings): BigQuery = {
    _bigquery match {
      case None =>
        val bqOptionsBuilder = BigQueryOptions.newBuilder()
        val credentials = getCredentials()
        val bqOptions = bqOptionsBuilder.setProjectId(projectId)
        val bqService = credentials
          .map(bqOptions.setCredentials)
          .getOrElse(
            bqOptions
          ) // GOOGLE_APPLICATION_CREDENTIALS should reference the service account in the path
          .build()
          .getService()
        _bigquery = Some(bqService)
        bqService
      case Some(bqService) =>
        bqService
    }
  }

  private def getPropertyOrEnv(envVar: String): scala.Option[String] =
    scala.Option(System.getProperty(envVar, System.getenv(envVar)))

  private var _bigquery: scala.Option[BigQuery] = None

  private def applyRLS(forceApply: Boolean)(implicit settings: Settings): Try[Unit] = {
    Try {
      if (forceApply || settings.comet.accessPolicies.apply) {
        cliConfig.outputTableId match {
          case None =>
            throw new RuntimeException("TableId must be defined in order to apply access policies.")
          case Some(outputTableId) => applyACL(outputTableId, cliConfig.acl)
        }
        prepareRLS().foreach { rlsStatement =>
          logger.info(s"Applying row level security $rlsStatement")
          new BigQueryNativeJob(cliConfig, rlsStatement, None).runInteractiveQuery() match {
            case Failure(e) =>
              throw e
            case Success(BigQueryJobResult(_, _, Some(job)))
                if job.getStatus.getExecutionErrors != null =>
              throw new RuntimeException(
                job.getStatus.getExecutionErrors.asScala.reverse.mkString(",")
              )
            case Success(job) =>
              logger.info(s"Job with id $job on Statement $rlsStatement succeeded")
          }
        }
      }
    }
  }

  private def getPolicyTag(
    location: String,
    projectId: String,
    taxonomy: String,
    taxonomyRef: String,
    accessPolicy: String
  ) = {
    val policyTagsRequest =
      ListPolicyTagsRequest.newBuilder().setParent(taxonomyRef).build()
    val policyTags =
      policyTagClient.listPolicyTags(policyTagsRequest)
    val policyTagRef =
      policyTags
        .iterateAll()
        .asScala
        .filter(_.getDisplayName == accessPolicy)
        .map(_.getName)
        .headOption
        .getOrElse(
          throw new Exception(
            s"PolicyTag $accessPolicy not found in Taxonomy $taxonomy in project $projectId in location $location"
          )
        )
    policyTagRef
  }

  def applyIamPolicyTags(iamPolicyTags: IamPolicyTags)(implicit settings: Settings): Try[Unit] = {
    Try {
      val (location, projectId, taxonomy, taxonomyRef) = getTaxonomy(policyTagClient)

      val groupedPoliciyTags: Map[String, List[IamPolicyTag]] =
        iamPolicyTags.iamPolicyTags.groupBy(_.policyTag)

      groupedPoliciyTags.foreach { case (policyTag, iamPolicyTags) =>
        val bindings = iamPolicyTags.map { iamPolicyTag =>
          val binding = Binding.newBuilder()
          binding.setRole(iamPolicyTag.role)
          // binding.setCondition()
          binding.addAllMembers(iamPolicyTag.members.asJava)
          binding.build()
        }
        val iamPolicy = IAMPolicy.newBuilder().addAllBindings(bindings.asJava).build()
        val policyTagId = getPolicyTag(location, projectId, taxonomy, taxonomyRef, policyTag)
        val request =
          SetIamPolicyRequest
            .newBuilder()
            .setResource(policyTagId)
            .setPolicy(iamPolicy)
            .setUpdateMask(FieldMask.newBuilder().addPaths("bindings").build())
            .build()
        val createdPolicy = policyTagClient.setIamPolicy(request)
        logger.info(createdPolicy.toString)
      }
    }
  }

  private def getTaxonomy(
    client: PolicyTagManagerClient
  )(implicit settings: Settings): (String, String, String, String) = {
    val taxonomyProjectId =
      if (settings.comet.accessPolicies.projectId == "invalid_project") {
        this.projectId
      } else
        settings.comet.accessPolicies.projectId

    val location = settings.comet.accessPolicies.location
    val taxonomy = settings.comet.accessPolicies.taxonomy
    if (location == "invalid_location")
      throw new Exception("accessPolicies.location not set")
    if (taxonomyProjectId == "invalid_project")
      throw new Exception("accessPolicies.projectId not set")
    if (taxonomy == "invalid_taxonomy")
      throw new Exception("accessPolicies.taxonomy not set")
    val taxonomyListRequest =
      ListTaxonomiesRequest
        .newBuilder()
        .setParent(s"projects/$taxonomyProjectId/locations/$location")
        .setPageSize(1000)
        .build()
    val taxonomyList = client.listTaxonomies(taxonomyListRequest)
    val taxonomyRef = taxonomyList
      .iterateAll()
      .asScala
      .filter(_.getDisplayName() == taxonomy)
      .map(_.getName)
      .headOption
      .getOrElse(
        throw new Exception(
          s"Taxonomy $taxonomy not found in project $taxonomyProjectId in location $location"
        )
      )
    (location, taxonomyProjectId, taxonomy, taxonomyRef)
  }

  private def applyCLS(forceApply: Boolean)(implicit
    settings: Settings
  ): Try[Unit] = {
    Try {
      if (forceApply || settings.comet.accessPolicies.apply) {
        cliConfig.starlakeSchema match {
          case None =>
          case Some(schema) =>
            val anyAccessPolicyToApply =
              schema.attributes.map(_.accessPolicy).count(_.isDefined) > 0
            if (anyAccessPolicyToApply) {
              val (location, projectId, taxonomy, taxonomyRef) =
                getTaxonomy(policyTagClient)
              val policyTagIds = mutable.Map.empty[String, String]
              val table = cliConfig.outputTableId match {
                case None =>
                  throw new RuntimeException("TableId must be defined in order to apply CLS")
                case Some(outputTableId) => bigquery().getTable(outputTableId)
              }
              val tableDefinition = table.getDefinition[StandardTableDefinition]
              val bqSchema = tableDefinition.getSchema()
              val bqFields = bqSchema.getFields.asScala.toList
              val attributesMap =
                schema.attributes.map(attr => (attr.getFinalName().toLowerCase, attr)).toMap
              val updatedFields = bqFields.map { field =>
                attributesMap.get(field.getName.toLowerCase) match {
                  case None =>
                    // Maybe an ignored field
                    logger.info(
                      s"Ignore this field ${schema.name}.${field.getName} during CLS application "
                    )
                    field
                  case Some(attr) =>
                    attr.accessPolicy match {
                      case None =>
                        field
                      case Some(accessPolicy) =>
                        val policyTagId = policyTagIds.getOrElse(
                          accessPolicy, {
                            val policyTagRef =
                              getPolicyTag(location, projectId, taxonomy, taxonomyRef, accessPolicy)
                            policyTagIds.put(accessPolicy, policyTagRef)
                            policyTagRef
                          }
                        )

                        val fieldPolicyTags =
                          scala
                            .Option(field.getPolicyTags)
                            .map(_.getNames.asScala.toList)
                            .getOrElse(Nil)
                        if (fieldPolicyTags.length == 1 && fieldPolicyTags.head == policyTagId)
                          field
                        else {
                          Field
                            .newBuilder(field.getName, field.getType, field.getSubFields)
                            .setPolicyTags(
                              PolicyTags.newBuilder().setNames(List(policyTagId).asJava).build()
                            )
                            .build()
                        }
                    }
                }
              }
              table.toBuilder
                .setDefinition(StandardTableDefinition.of(BQSchema.of(updatedFields: _*)))
                .build()
                .update()
            }
        }
      }

    }
  }

  def prepareRLS(): List[String] = {
    def revokeAllPrivileges(): String = {
      cliConfig.outputTableId match {
        case Some(outputTableId) =>
          val outputTable = BigQueryJobBase.getBqTableForNative(outputTableId)
          s"DROP ALL ROW ACCESS POLICIES ON `$outputTable`"
        case None =>
          throw new RuntimeException("TableId must be defined in order to revoke privileges")
      }
    }

    /** Grant privileges to the users and groups defined in the schema
      * @param rlsRetrieved
      * @return
      */
    def grantPrivileges(rlsRetrieved: RowLevelSecurity): String = {
      cliConfig.outputTableId match {
        case None =>
          throw new RuntimeException("TableId must be defined in order to grant privileges")
        case Some(outputTableId) =>
          val grants = rlsRetrieved.grantees().map {
            case (UserType.SA, u) =>
              s"serviceAccount:$u"
            case (userOrGroupOrDomainType, userOrGroupOrDomainName) =>
              s"${userOrGroupOrDomainType.toString.toLowerCase}:$userOrGroupOrDomainName"
          }

          val name = rlsRetrieved.name
          val filter = rlsRetrieved.predicate
          val outputTable = BigQueryJobBase.getBqTableForNative(outputTableId)
          s"""
             | CREATE ROW ACCESS POLICY
             |  $name
             | ON
             |  `$outputTable`
             | GRANT TO
             |  (${grants.mkString("\"", "\",\"", "\"")})
             | FILTER USING
             |  ($filter)
             |""".stripMargin
      }
    }
    val rlsCreateStatements = cliConfig.rls.map { rlsRetrieved =>
      logger.info(s"Building security statement $rlsRetrieved")
      val rlsCreateStatement = grantPrivileges(rlsRetrieved)
      logger.info(s"An access policy will be created using $rlsCreateStatement")
      rlsCreateStatement
    }

    val rlsDeleteStatement = cliConfig.rls.map(_ => revokeAllPrivileges())

    rlsDeleteStatement ++ rlsCreateStatements
  }

  lazy val tableId: TableId = {
    cliConfig.outputTableId.getOrElse(throw new Exception("TableId must be defined"))
  }

  protected lazy val datasetId: DatasetId = BigQueryJobBase.getBqDatasetId(tableId)

  protected lazy val bqTable: String = BigQueryJobBase.getBqTableForSpark(tableId)

  protected lazy val bqNativeTable: String = BigQueryJobBase.getBqTableForNative(tableId)

  /** Retry on retryable bigquery exception.
    */
  private def recoverBigqueryException[T](bigqueryProcess: => T): Try[T] = {
    def processWithRetry(retry: Int = 0, bigqueryProcess: => T): Try[T] = {
      Try {
        bigqueryProcess
      }.recoverWith {
        case be: BigQueryException
            if retry < 3 && scala
              .Option(be.getError)
              .exists(_.getReason() == "rateLimitExceeded") =>
          val sleepTime = 5000 * (retry + 1) + SecureRandom.getInstanceStrong.nextInt(5000)
          logger.error(s"Retry in $sleepTime. ${be.getMessage}")
          Thread.sleep(sleepTime)
          processWithRetry(retry + 1, bigqueryProcess)
        case be: BigQueryException
            if retry < 3 && scala.Option(be.getError).exists(_.getReason() == "duplicate") =>
          logger.error(be.getMessage)
          processWithRetry(retry + 1, bigqueryProcess)
      }
    }
    processWithRetry(bigqueryProcess = bigqueryProcess)
  }

  def tableExists(tableId: TableId)(implicit settings: Settings): Boolean = {
    try {
      val table = bigquery().getTable(tableId)
      table != null && table.exists
    } catch {
      case _: BigQueryException =>
        false
    }
  }

  def tableExists(databaseName: scala.Option[String], datasetName: String, tableName: String)(
    implicit settings: Settings
  ): Boolean =
    tableExists(getTableId(databaseName, datasetName, tableName))

  def dropTable(tableId: TableId)(implicit
    settings: Settings
  ): Boolean = {
    val success = bigquery().delete(tableId)
    if (success)
      logger.info(s"Table $tableId deleted")
    else
      logger.info(s"Table $tableId not found")
    success
  }
  def dropTable(databaseName: scala.Option[String], datasetName: String, tableName: String)(implicit
    settings: Settings
  ): Boolean = {
    val tableId = getTableId(databaseName, datasetName, tableName)
    dropTable(tableId)
  }

  private def getTableId(
    databaseName: scala.Option[String],
    datasetName: String,
    tableName: String
  ): TableId = {
    databaseName match {
      case Some(dbName) =>
        TableId.of(dbName, datasetName, tableName)
      case None => TableId.of(datasetName, tableName)
    }
  }

  def getBQSchema(tableId: TableId)(implicit settings: Settings): BQSchema = {
    val table = bigquery().getTable(tableId)
    assert(table.exists)
    table.getDefinition[StandardTableDefinition].getSchema
  }

  def updateTableSchema(tableId: TableId, tableSchema: BQSchema)(implicit
    settings: Settings
  ): Try[StandardTableDefinition] = {
    Try {
      val table = bigquery().getTable(tableId)
      val newTableDefinition = StandardTableDefinition.newBuilder().setSchema(tableSchema).build()
      table.toBuilder.setDefinition(newTableDefinition).build().update()
      newTableDefinition
    }
  }

  /** Get or create a BigQuery dataset
    *
    * @param domainDescription
    *   the domain description
    * @return
    *   the dataset
    */
  def getOrCreateDataset(
    domainDescription: scala.Option[String]
  )(implicit settings: Settings): Try[Dataset] = {
    val tryResult = recoverBigqueryException {
      val existingDataset = scala.Option(bigquery().getDataset(datasetId))
      val labels = Utils.extractTags(cliConfig.domainTags).toMap
      existingDataset match {
        case Some(ds) =>
          updateDatasetInfo(ds, domainDescription, labels)
        case None =>
          val datasetInfo = DatasetInfo
            .newBuilder(datasetId)
            .setLocation(connectionOptions.flatMap(_.get("location")).getOrElse("EU"))
            .setDescription(domainDescription.orNull)
            .setLabels(labels.asJava)
            .build
          bigquery().create(datasetInfo)
      }
    }
    tryResult match {
      case Failure(exception) =>
        logger.error(s"Dataset ${datasetId.getDataset} was not created / retrieved.")
        Utils.logException(logger, exception)
        tryResult
      case Success(_) => tryResult
    }
  }

  /** Get or create a BigQuery table
    *
    * @param domainDescription
    *   the domain description
    * @param tableInfo
    *   the table info
    * @param maybeSourceDF
    *   the source dataframe
    * @return
    *   the table and the source dataframe
    */
  def getOrCreateTable(
    domainDescription: scala.Option[String],
    tableInfo: model.TableInfo,
    dataFrame: scala.Option[DataFrame]
  )(implicit settings: Settings): Try[(Table, StandardTableDefinition)] = {
    getOrCreateDataset(domainDescription).flatMap { _ =>
      val tryResult = recoverBigqueryException {

        val table =
          if (tableExists(tableId)) {
            val table = bigquery().getTable(tableId)
            updateTableDescription(table, tableInfo.maybeTableDescription.orNull)
          } else {
            val tableDefinition = newTableDefinition(tableInfo, dataFrame)
            val bqTableInfoBuilder = BQTableInfo
              .newBuilder(tableId, tableDefinition)
              .setDescription(tableInfo.maybeTableDescription.orNull)

            if (tableInfo.maybePartition.isEmpty) {
              cliConfig.days match {
                case Some(days) =>
                  bqTableInfoBuilder.setExpirationTime(
                    System.currentTimeMillis() + days * 24 * 3600 * 1000
                  )
                case None =>
              }
            }

            val bqTableInfo = bqTableInfoBuilder.build
            val result = bigquery().create(bqTableInfo)
            logger.info(s"Table ${tableId.getDataset}.${tableId.getTable} created successfully")
            result
          }
        setTagsOnTable(table)
        (table, table.getDefinition[StandardTableDefinition])
      }
      tryResult match {
        case Failure(exception) =>
          logger.info(
            s"Table ${tableId.getDataset}.${tableId.getTable} was not created / retrieved."
          )
          Utils.logException(logger, exception)
          tryResult
        case Success(_) => tryResult
      }
    }
  }

  protected def setTagsOnTable(table: Table): Unit = {
    cliConfig.starlakeSchema.foreach { schema =>
      val tableTagPairs = Utils.extractTags(schema.tags)
      table.toBuilder.setLabels(tableTagPairs.toMap.asJava).build().update()
    }
  }

  protected def updateTableDescription(bqTable: Table, description: String)(implicit
    settings: Settings
  ): Table = {
    recoverBigqueryException {
      if (scala.Option(bqTable.getDescription) != scala.Option(description)) {
        // Update dataset description only when description is explicitly set
        logger.info("Table's description has changed")
        bqTable.toBuilder.setDescription(description).build().update()
      } else {
        logger.info("Table's description has not changed")
        bqTable
      }
    } match {
      case Failure(exception) =>
        throw exception
      case Success(table) => table
    }
  }

  private def updateDatasetInfo(
    dataset: Dataset,
    description: scala.Option[String],
    labels: Map[String, String]
  ): Dataset = {
    val alterDescriptionIfChanged = (datasetBuilder: Dataset.Builder, changeState: Boolean) => {
      if (description.isDefined && scala.Option(dataset.getDescription) != description) {
        // Update dataset description only when description is explicitly set
        logger.info("Dataset's description has changed")
        datasetBuilder.setDescription(description.orNull) -> true
      } else
        datasetBuilder -> changeState
    }
    val alterLabelsIfChanged = (datasetBuilder: Dataset.Builder, changeState: Boolean) => {
      val existingLabels =
        scala.Option(dataset.getLabels).map(_.asScala).getOrElse(Map.empty[String, String])
      if (existingLabels.size != labels.size || existingLabels != labels) {
        logger.info("Dataset's labels has changed")
        datasetBuilder.setLabels(labels.asJava) -> true
      } else
        datasetBuilder -> changeState
    }
    val (builder, changed) =
      List(alterDescriptionIfChanged, alterLabelsIfChanged).foldLeft(dataset.toBuilder -> false) {
        case ((builder, state), alterFunc) => alterFunc(builder, state)
      }
    if (changed) {
      logger.info("Updating dataset")
      builder.build().update()
    } else {
      logger.info("No metadata change for dataset")
      dataset
    }
  }

  /** Parses the input sql parameter to find all the referenced tables and finds the description of
    * the columns of these tables based on the description found in the existing BigQuery tables and
    * AttributeDesc comments provided in the Job description. The result is returned as a
    * Map[String,String] with the column name as key, and its description as value.
    * @param sql
    * @return
    *   a Map with all the
    */
  def getFieldsDescriptionSource(sql: String)(implicit settings: Settings): Map[String, String] = {
    val tableIds = SQLUtils
      .extractRefsInSQL(sql)
      .flatMap(table => {
        val infos = table.split("\\.").toList
        infos match {
          case datasetName :: tableName :: Nil =>
            Some(TableId.of(projectId, datasetName, tableName))
          case projectId :: datasetName :: tableName :: Nil =>
            Some(TableId.of(projectId, datasetName, tableName))
          case _ => None
        }
      })

    tableIds
      .flatMap(tableId =>
        Try(bigquery().getTable(tableId))
          .map { table =>
            logger.info(s"Get table source description field : ${table.getTableId.toString}")
            table
              .getDefinition[StandardTableDefinition]
              .getSchema
              .getFields
              .iterator()
              .asScala
              .toList
          }
          .getOrElse(Nil)
      )

      /** We expect that two tables with the same column name have exactly the same comment or else
        * we pick anyone by default
        */
      .map(field => field.getName -> field.getDescription)
      .toMap ++
    cliConfig.attributesDesc
      .filter(_.comment.nonEmpty)
      .map(att => att.name -> att.comment)
      .toMap
  }

  /** Update columns description from source tables in sql or from yaml job with attributes
    * information
    * @param dictField
    *   Map of columns and their descriptions
    * @return
    *   Table with columns description updated
    */
  def updateColumnsDescription(
    dictField: Map[String, String]
  )(implicit settings: Settings): Table = {
    recoverBigqueryException {
      val tableTarget = bigquery().getTable(tableId)
      val tableSchema = tableTarget.getDefinition.asInstanceOf[StandardTableDefinition].getSchema
      val (fieldList, descriptionChanged) = tableSchema.getFields
        .iterator()
        .asScala
        .toList
        .foldLeft(List[Field]() -> false) { case ((fields, changed), field) =>
          val targetDescription = dictField.getOrElse(field.getName, field.getDescription)
          val fieldDescriptionHasChange =
            scala.Option(targetDescription) != scala.Option(field.getDescription)
          (fields :+ field.toBuilder
            .setDescription(targetDescription)
            .build()) -> (changed || fieldDescriptionHasChange)
        }
      if (descriptionChanged) {
        logger.info(s"$bqTable's column description has changed")
        bigquery.update(
          tableTarget.toBuilder
            .setDefinition(StandardTableDefinition.of(BQSchema.of(fieldList.asJava)))
            .build()
        )
      } else {
        logger.info(s"$bqTable's column description has not changed")
        tableTarget
      }
    } match {
      case Failure(exception) =>
        throw exception
      case Success(table) => table
    }
  }

  /** To set access control on a table or view, we can use Identity and Access Management (IAM)
    * policy After you create a table or view, you can set its policy with a set-iam-policy call For
    * each call, we compare if the existing policy is equal to the defined one (in the Yaml file) If
    * it's the case, we do nothing, otherwise we update the Table policy
    * @param tableId
    * @param acl
    * @return
    */
  private def applyACL(
    tableId: TableId,
    acl: List[AccessControlEntry]
  )(implicit settings: Settings): Policy = {
    // val BIG_QUERY_VIEWER_ROLE = "roles/bigquery.dataViewer"
    val existingPolicy: Policy = bigquery().getIamPolicy(tableId)
    val existingPolicyBindings: util.Map[Role, util.Set[Identity]] = existingPolicy.getBindings

    val bindings = acl
      .groupBy(_.role)
      .map { case (role, listAcl) =>
        Role.of(role) -> listAcl.flatMap(_.grants).toSet.map(Identity.valueOf).asJava
      }
      .asJava

    if (!existingPolicyBindings.equals(bindings)) {
      logger.info(
        s"We are updating the IAM Policy on this Table: $tableId with new Policies"
      )
      val editedPolicy: Policy = existingPolicy.toBuilder
        .setBindings(
          bindings
        )
        .build()
      bigquery().setIamPolicy(tableId, editedPolicy)
      editedPolicy
    } else {
      logger.info(s"Iam Policy is the same as before on this Table: $tableId")
      existingPolicy
    }
  }

  private def newTableDefinition(
    tableInfo: model.TableInfo,
    dataFrame: scala.Option[DataFrame]
  ): TableDefinition = {
    val maybeTimePartitioning = tableInfo.maybePartition
      .map(partitionInfo =>
        timePartitioning(
          partitionInfo.field,
          partitionInfo.expirationDays,
          partitionInfo.requirePartitionFilter
        ).build()
      )
    val withPartitionDefinition = tableInfo.maybeSchema match {
      case Some(schema) =>
        val tableConstraints: TableConstraints = getTableConstraints()
        StandardTableDefinition
          .newBuilder()
          .setSchema(schema)
          .setTableConstraints(tableConstraints)
          .setTimePartitioning(maybeTimePartitioning.orNull)
      case None =>
        // We would have loved to let BQ do the whole job (StandardTableDefinition.newBuilder())
        // But however seems like it does not work when there is an output partition
        val tableDefinition =
          StandardTableDefinition
            .newBuilder()
            .setTimePartitioning(maybeTimePartitioning.orNull)
        dataFrame
          .map(dataFrame => tableDefinition.setSchema(sparkToBq(dataFrame)))
          .getOrElse(tableDefinition)
    }
    val withClusteringDefinition = tableInfo.maybeCluster match {
      case Some(clusterConfig) =>
        val clustering = Clustering.newBuilder().setFields(clusterConfig.fields.asJava).build()
        withPartitionDefinition.setClustering(clustering)
        withPartitionDefinition
      case None => withPartitionDefinition
    }
    withClusteringDefinition.build()
  }

  private def getTableConstraints(): TableConstraints = {
    val tableConstraints = cliConfig.starlakeSchema match {
      case Some(starlakeSchema) =>
        val pkTableConstraints = if (starlakeSchema.primaryKey.nonEmpty) {
          val primaryKey =
            PrimaryKey.newBuilder().setColumns(starlakeSchema.primaryKey.asJava).build()
          TableConstraints.newBuilder().setPrimaryKey(primaryKey);
        } else {
          TableConstraints.newBuilder();
        }
        val fkComponents = starlakeSchema.attributes.flatMap { attr =>
          starlakeSchema.fkComponents(attr, datasetId.getDataset)
        }
        val foreignKeys = fkComponents.map { case (attr, domain, table, referencedColumn) =>
          val columnReference =
            ColumnReference.newBuilder
              .setReferencingColumn(attr.getFinalName())
              .setReferencedColumn(referencedColumn)
              .build

          val tableIdPk =
            TableId.of(cliConfig.outputDatabase.getOrElse(this.projectId), domain, table)
          ForeignKey.newBuilder
            .setName("foreign_key")
            .setColumnReferences(List(columnReference).asJava)
            .setReferencedTable(tableIdPk)
            .build
        }
        pkTableConstraints.setForeignKeys(foreignKeys.asJava)
      case None =>
        TableConstraints.newBuilder();
    }
    tableConstraints.build()
  }

  def timePartitioning(
    partitionField: String,
    days: scala.Option[Int] = None,
    requirePartitionFilter: Boolean
  ): TimePartitioning.Builder = {
    val partitionFilter = TimePartitioning
      .newBuilder(TimePartitioning.Type.DAY)
      .setRequirePartitionFilter(requirePartitionFilter)
    val partitioned =
      if (!Set("_PARTITIONTIME", "_PARTITIONDATE").contains(partitionField.toUpperCase))
        partitionFilter.setField(partitionField)
      else
        partitionFilter

    days match {
      case Some(d) =>
        partitioned.setExpirationMs(d * 24 * 3600 * 1000L)
      case _ =>
        partitioned
    }
  }

  def getTableDefinition(tableId: TableId)(implicit settings: Settings): StandardTableDefinition = {
    bigquery().getTable(tableId).getDefinition[StandardTableDefinition]
  }
}

object BigQueryJobBase {

  private def getBqDatasetId(tableId: TableId): DatasetId = {
    scala.Option(tableId.getProject) match {
      case Some(_) => DatasetId.of(tableId.getProject, tableId.getDataset)
      case None    => DatasetId.of(tableId.getDataset)
    }
  }

  def getBqDatasetForNative(tableId: TableId): String = {
    val projectId = getProjectIdPrefix(tableId.getProject, ".")
    s"${projectId}${tableId.getDataset}"
  }

  def getBqTableForSpark(tableId: TableId): String = {
    val projectId = getProjectIdPrefix(tableId.getProject, ":")
    s"${projectId}${tableId.getDataset}.${tableId.getTable}"
  }
  def getBqTableForNative(tableId: TableId): String = {
    val projectId = getProjectIdPrefix(tableId.getProject, ".")
    s"${projectId}${tableId.getDataset}.${tableId.getTable}"
  }

  def extractProjectDatasetAndTable(
    databaseId: scala.Option[String],
    datasetId: String,
    tableId: String
  ): TableId = {
    databaseId match {
      case Some(dbId) =>
        BigQueryJobBase.extractProjectDatasetAndTable(dbId + ":" + datasetId + "." + tableId)
      case None =>
        BigQueryJobBase.extractProjectDatasetAndTable(datasetId + "." + tableId)
    }
  }

  /** @param resourceId
    *   resourceId is a an id of one of the following pattern: <projectId>:<datasetId>.<tableId> or
    *   <datasetId>.<tableId>
    * @return
    */
  def extractProjectDatasetAndTable(resourceId: String): TableId = {
    def extractDatasetAndTable(str: String): (String, String) = {
      val sepIndex = str.indexOf('.')
      if (sepIndex > 0)
        (str.substring(0, sepIndex), str.substring(sepIndex + 1))
      else
        throw new Exception(s"Dataset cannot be null in BigQuery view name ($resourceId)")
    }

    val sepIndex = resourceId.indexOf(":")
    val (project, (dataset, table)) =
      if (sepIndex > 0)
        (
          Some(resourceId.substring(0, sepIndex)),
          extractDatasetAndTable(resourceId.substring(sepIndex + 1))
        )
      else // parquet is the default
        (None, extractDatasetAndTable(resourceId))

    project
      .map(project => TableId.of(project, dataset, table))
      .getOrElse(TableId.of(ServiceOptions.getDefaultProjectId(), dataset, table))
  }

  def extractProjectDataset(value: String): DatasetId = {
    val sepIndex = value.indexOf(":")
    val (project, dataset) =
      if (sepIndex > 0)
        (Some(value.substring(0, sepIndex)), value.substring(sepIndex + 1))
      else // parquet is the default
        (None, value)

    project
      .map(project => DatasetId.of(project, dataset))
      .getOrElse(DatasetId.of(ServiceOptions.getDefaultProjectId(), dataset))
  }

  private def getProjectIdPrefix(nullableProjectId: String, separator: String): String = {
    scala.Option(nullableProjectId).map(_ + separator).getOrElse("")
  }
}
