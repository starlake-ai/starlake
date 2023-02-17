package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.schema.model
import ai.starlake.schema.model.{
  AccessControlEntry,
  IamPolicyTag,
  IamPolicyTags,
  RowLevelSecurity,
  UserType
}
import ai.starlake.utils.Utils
import ai.starlake.utils.conversion.BigQueryUtils.sparkToBq
import com.google.cloud.bigquery.{Schema => BQSchema, TableInfo => BQTableInfo, _}
import com.google.cloud.datacatalog.v1.{
  ListPolicyTagsRequest,
  ListTaxonomiesRequest,
  PolicyTagManagerClient,
  PolicyTagName
}
import com.google.cloud.{Identity, Policy, Role, ServiceOptions}
import com.google.iam.v1.{Binding, Policy => IAMPolicy, SetIamPolicyRequest}
import com.google.protobuf.FieldMask
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

trait BigQueryJobBase extends StrictLogging {
  def cliConfig: BigQueryLoadConfig

  def projectId: String =
    cliConfig.gcpProjectId.getOrElse(ServiceOptions.getDefaultProjectId())

  // Lazy otherwise tests fail since there is no GCP credentials in test mode
  lazy val policyTagClient: PolicyTagManagerClient = PolicyTagManagerClient.create()

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
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
          cliConfig.outputDataset,
          cliConfig.outputTable
        )
        applyACL(tableId, cliConfig.acl)
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
        val request =
          SetIamPolicyRequest
            .newBuilder()
            .setResource(
              PolicyTagName.of(projectId, location, taxonomy, policyTag).toString()
            )
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
              val tableId =
                BigQueryJobBase.extractProjectDatasetAndTable(
                  cliConfig.outputDataset,
                  cliConfig.outputTable
                )
              val table: Table = bigquery().getTable(tableId)
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
                            val policyTagsRequest =
                              ListPolicyTagsRequest.newBuilder().setParent(taxonomyRef).build()
                            val policyTags =
                              policyTagClient.listPolicyTags(policyTagsRequest)
                            val policyTagRef =
                              policyTags
                                .iterateAll()
                                .asScala
                                .filter(_.getDisplayName() == accessPolicy)
                                .map(_.getName)
                                .headOption
                                .getOrElse(
                                  throw new Exception(
                                    s"PolicyTag $accessPolicy not found in Taxonomy $taxonomy in project $projectId in location $location"
                                  )
                                )
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

      s"DROP ALL ROW ACCESS POLICIES ON ${cliConfig.outputDataset}.${cliConfig.outputTable}"
    }

    def grantPrivileges(rlsRetrieved: RowLevelSecurity): String = {
      val grants = rlsRetrieved.grantees().map {
        case (UserType.SA, u) =>
          s"serviceAccount:$u"
        case (userOrGroupOrDomainType, userOrGroupOrDomainName) =>
          s"${userOrGroupOrDomainType.toString.toLowerCase}:$userOrGroupOrDomainName"
      }

      val name = rlsRetrieved.name
      val filter = rlsRetrieved.predicate
      s"""
         | CREATE ROW ACCESS POLICY
         |  $name
         | ON
         |  ${cliConfig.outputDataset}.${cliConfig.outputTable}
         | GRANT TO
         |  (${grants.mkString("\"", "\",\"", "\"")})
         | FILTER USING
         |  ($filter)
         |""".stripMargin
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

  val datasetId: DatasetId = BigQueryJobBase.extractProjectDataset(cliConfig.outputDataset)

  val tableId: TableId = BigQueryJobBase.extractProjectDatasetAndTable(
    cliConfig.outputDataset + "." + cliConfig.outputTable
  )

  val bqTable = s"${cliConfig.outputDataset}.${cliConfig.outputTable}"

  def getOrCreateDataset()(implicit settings: Settings): Dataset = {
    val existingDataset = scala.Option(bigquery().getDataset(datasetId))
    val dataset = existingDataset.getOrElse {
      val datasetInfo = DatasetInfo
        .newBuilder(BigQueryJobBase.extractProjectDataset(cliConfig.outputDataset))
        .setLocation(cliConfig.getLocation())
        .build
      bigquery().create(datasetInfo)
    }
    setTagsOnDataset(dataset)
    dataset
  }

  def getOrCreateTable(
    tableInfo: model.TableInfo,
    dataFrame: scala.Option[DataFrame]
  )(implicit settings: Settings): Try[(Table, StandardTableDefinition)] = {
    val tryResult = Try {
      getOrCreateDataset()

      val tableDefinition = getTableDefinition(tableInfo, dataFrame)
      val table = scala
        .Option(bigquery().getTable(tableId))
        .map(t => {
          val out =
            updateTableDescriptions(t, tableInfo.maybeTableDescription, Some(tableDefinition))
          logger.info(s"Table ${tableId.getDataset}.${tableId.getTable} updated successfully")
          out
        }) getOrElse {
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
        logger.info(s"Table ${tableId.getDataset}.${tableId.getTable} was not created.")
        Utils.logException(logger, exception)
        tryResult
      case Success(_) => tryResult
    }
  }

  protected def setTagsOnTable(table: Table): Unit = {
    cliConfig.starlakeSchema.foreach { schema =>
      val tableTagPairs = Utils.extractTags(schema.tags)
      table.toBuilder.setLabels(tableTagPairs.toMap.asJava).build().update()
    }
  }

  protected def updateTableDescriptions(
    table: Table,
    maybeTableDescription: scala.Option[String],
    maybeTableDefinition: scala.Option[TableDefinition]
  ): Table = {
    table.toBuilder
      .setDescription(maybeTableDescription.orNull)
      .setDefinition(maybeTableDefinition.orNull)
      .build()
      .update()
  }

  protected def setTagsOnDataset(dataset: Dataset): Unit = {
    val datasetTagPairs = Utils.extractTags(cliConfig.domainTags)
    dataset.toBuilder.setLabels(datasetTagPairs.toMap.asJava).build().update()
  }

  /** To set access control on a table or view, we can use Identity and Access Management (IAM)
    * policy After you create a table or view, you can set its policy with a set-iam-policy call For
    * each call, we compare if the existing policy is equal to the defined one (in the Yaml file) If
    * it's the case, we do nothing, otherwise we update the Table policy
    * @param tableId
    * @param acl
    * @return
    */
  def applyACL(
    tableId: TableId,
    acl: List[AccessControlEntry]
  )(implicit settings: Settings): Policy = {
    // val BIG_QUERY_VIEWER_ROLE = "roles/bigquery.dataViewer"
    val existingPolicy: Policy = bigquery().getIamPolicy(tableId)
    val existingPolicyBindings: util.Map[Role, util.Set[Identity]] = existingPolicy.getBindings

    val bindings = acl
      .map { ace =>
        Role.of(ace.role) -> ace.grants.toSet.map(Identity.valueOf).asJava
      }
      .toMap
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
}
