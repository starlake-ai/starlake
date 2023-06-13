package ai.starlake.job.sink.bigquery
import ai.starlake.config.Settings
import ai.starlake.schema.model
import ai.starlake.schema.model.{Schema => _, TableInfo => _, _}
import ai.starlake.utils.conversion.BigQueryUtils.sparkToBq
import ai.starlake.utils.{SQLUtils, Utils}
import com.google.cloud.bigquery.{Schema => BQSchema, TableInfo => BQTableInfo, _}
import com.google.cloud.datacatalog.v1.{
  ListPolicyTagsRequest,
  ListTaxonomiesRequest,
  PolicyTagManagerClient
}
import com.google.cloud.{Identity, Policy, Role, ServiceOptions}
import com.google.iam.v1.{Binding, Policy => IAMPolicy, SetIamPolicyRequest}
import com.google.protobuf.FieldMask
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame

import java.security.SecureRandom
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Base class for BigQuery jobs
  */
trait BigQueryJobBase extends StrictLogging {
  def cliConfig: BigQueryLoadConfig

  def projectId: String =
    cliConfig.gcpProjectId.getOrElse(ServiceOptions.getDefaultProjectId())

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
        val bqService = cliConfig.bigquery()
        _bigquery = Some(bqService)
        bqService
      case Some(bqService) =>
        bqService
    }
  }

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
          new BigQueryNativeJob(cliConfig, rlsStatement, None).runBatchQuery() match {
            case Failure(e) =>
              throw e
            case Success(job) if job.getStatus.getExecutionErrors != null =>
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
          val outputTable = BigQueryJobBase.getBqNativeTable(outputTableId)
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
          val outputTable = BigQueryJobBase.getBqNativeTable(outputTableId)
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

  protected lazy val tableId: TableId = {
    cliConfig.outputTableId.getOrElse(throw new Exception("TableId must be defined"))
  }

  protected lazy val datasetId: DatasetId = BigQueryJobBase.getBqDatasetId(tableId)

  protected lazy val bqTable: String = BigQueryJobBase.getBqTable(tableId)

  protected lazy val bqNativeTable: String = BigQueryJobBase.getBqNativeTable(tableId)

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
            .setLocation(cliConfig.getLocation())
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

        val tableDefinition = getTableDefinition(tableInfo, dataFrame)
        val table = scala
          .Option(bigquery().getTable(tableId))
          .getOrElse {
            val bqTableInfo = BQTableInfo
              .newBuilder(tableId, tableDefinition)
              .setDescription(tableInfo.maybeTableDescription.orNull)
              .build
            val result = bigquery().create(
              bqTableInfo
            )
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

  private def setTagsOnTable(table: Table): Unit = {
    cliConfig.starlakeSchema.foreach { schema =>
      val tableTagPairs = Utils.extractTags(schema.tags)
      table.toBuilder.setLabels(tableTagPairs.toMap.asJava).build().update()
    }
  }

  protected def updateTableDescription(idTable: TableId, description: String)(implicit
    settings: Settings
  ): Table = {
    recoverBigqueryException {
      val bqTable = bigquery().getTable(idTable)
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
      .extractRefsFromSQL(sql)
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
          .map(table => {
            logger.info(s"Get table source description field : ${table.getTableId.toString}")
            table
              .getDefinition[StandardTableDefinition]
              .getSchema
              .getFields
              .iterator()
              .asScala
              .toList
          })
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

  private def getTableDefinition(
    tableInfo: model.TableInfo,
    dataFrame: scala.Option[DataFrame]
  ): TableDefinition = {
    val maybeTimePartitioning = tableInfo.maybePartition.map(partitionInfo =>
      timePartitioning(
        partitionInfo.field,
        partitionInfo.expirationDays,
        partitionInfo.requirePartitionFilter
      ).build()
    )
    val withPartitionDefinition = tableInfo.maybeSchema match {
      case Some(schema) =>
        StandardTableDefinition
          .newBuilder()
          .setSchema(schema)
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

  def timePartitioning(
    partitionField: String,
    days: scala.Option[Int] = None,
    requirePartitionFilter: Boolean
  ): TimePartitioning.Builder = {
    days match {
      case Some(d) =>
        TimePartitioning
          .newBuilder(TimePartitioning.Type.DAY)
          .setField(partitionField)
          .setExpirationMs(d * 3600 * 24 * 1000L)
          .setRequirePartitionFilter(requirePartitionFilter)
      case _ =>
        TimePartitioning
          .newBuilder(TimePartitioning.Type.DAY)
          .setField(partitionField)
          .setRequirePartitionFilter(requirePartitionFilter)
    }
  }
}

object BigQueryJobBase {

  private def getBqDatasetId(tableId: TableId): DatasetId = {
    scala.Option(tableId.getProject) match {
      case Some(_) => DatasetId.of(tableId.getProject, tableId.getDataset)
      case None    => DatasetId.of(tableId.getDataset)
    }
  }
  def getBqDataset(tableId: TableId): String = {
    val projectId = getProjectIdPrefix(tableId.getProject, ":")
    s"${projectId}${tableId.getDataset}"
  }
  def getBqNativeDataset(tableId: TableId): String = {
    val projectId = getProjectIdPrefix(tableId.getProject, ".")
    s"${projectId}${tableId.getDataset}"
  }

  def getBqTable(tableId: TableId): String = {
    val projectId = getProjectIdPrefix(tableId.getProject, ":")
    s"${projectId}${tableId.getDataset}.${tableId.getTable}"
  }
  def getBqNativeTable(tableId: TableId): String = {
    val projectId = getProjectIdPrefix(tableId.getProject, ".")
    s"${projectId}${tableId.getDataset}.${tableId.getTable}"
  }

  def extractProjectDatasetAndTable(datasetId: String, tableId: String): TableId = {
    BigQueryJobBase.extractProjectDatasetAndTable(datasetId + "." + tableId)
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
      .getOrElse(TableId.of(dataset, table))
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
      .getOrElse(DatasetId.of(dataset))
  }

  private def getProjectIdPrefix(nullableProjectId: String, seperator: String): String = {
    scala.Option(nullableProjectId).map(_ + seperator).getOrElse("")
  }
}
