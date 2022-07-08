package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQueryJobBase.extractProjectDataset
import ai.starlake.schema.model.{AccessControlEntry, RowLevelSecurity, UserType}
import ai.starlake.utils.Utils
import com.google.cloud.bigquery.{Schema => BQSchema, _}
import com.google.cloud.datacatalog.v1.{
  ListPolicyTagsRequest,
  ListTaxonomiesRequest,
  PolicyTagManagerClient
}
import com.google.cloud.{Identity, Policy, Role}
import com.typesafe.scalalogging.StrictLogging

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

trait BigQueryJobBase extends StrictLogging {
  def cliConfig: BigQueryLoadConfig
  def projectId: String

  def applyRLSAndCLS(forceApply: Boolean = false)(implicit settings: Settings): Try[Unit] = {
    for {
      _ <- applyRLS(forceApply)
      _ <- applyCLS(forceApply)
    } yield ()
  }

  private def applyRLS(forceApply: Boolean)(implicit settings: Settings): Try[Unit] = {
    Try {
      if (forceApply || settings.comet.accessPolicies.apply) {
        val tableId = TableId.of(cliConfig.outputDataset, cliConfig.outputTable)
        cliConfig.acl.foreach(acl => applyACL(tableId, acl))
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
              logger.info(s"Job with id ${job} on Statement $rlsStatement succeeded")
          }
        }
      }
    }
  }

  private def getTaxonomy(
    client: PolicyTagManagerClient
  )(implicit settings: Settings): (String, String, String, String) = {
    val projectId = {
      if (settings.comet.accessPolicies.projectId == "invalid_project")
        if (sys.env.contains("GCLOUD_PROJECT"))
          sys.env("GCLOUD_PROJECT")
        else if (sys.env.contains("GOOGLE_CLOUD_PROJECT"))
          sys.env("GOOGLE_CLOUD_PROJECT")
        else
          "invalid_project"
      else
        settings.comet.accessPolicies.projectId
    }

    val location = settings.comet.accessPolicies.location
    val taxonomy = settings.comet.accessPolicies.taxonomy
    if (location == "invalid_location")
      throw new Exception("accessPolicies.location not set")
    if (projectId == "invalid_project")
      throw new Exception("accessPolicies.projectId not set")
    if (taxonomy == "invalid_taxonomy")
      throw new Exception("accessPolicies.taxonomy not set")
    val taxonomyListRequest =
      ListTaxonomiesRequest
        .newBuilder()
        .setParent(s"projects/$projectId/locations/$location")
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
          s"Taxonomy $taxonomy not found in project $projectId in location $location"
        )
      )
    (location, projectId, taxonomy, taxonomyRef)
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
                getTaxonomy(BigQueryJobBase.policyTagClient)
              val policyTagIds = mutable.Map.empty[String, String]
              val tableId = TableId.of(cliConfig.outputDataset, cliConfig.outputTable)
              val table: Table = BigQueryJobBase.bigquery.getTable(tableId)
              val tableDefinition = table.getDefinition().asInstanceOf[StandardTableDefinition]
              val bqSchema = tableDefinition.getSchema()
              val bqFields = bqSchema.getFields.asScala.toList
              val attributesMap = schema.attributes.map(attr => (attr.name.toLowerCase, attr)).toMap
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
                              BigQueryJobBase.policyTagClient.listPolicyTags(policyTagsRequest)
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
        case (userOrGroupType, userOrGroupName) =>
          s"${userOrGroupType.toString.toLowerCase}:$userOrGroupName"
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
    val rlsCreateStatements = cliConfig.rls.getOrElse(Nil).map { rlsRetrieved =>
      logger.info(s"Building security statement $rlsRetrieved")
      val rlsCreateStatement = grantPrivileges(rlsRetrieved)
      logger.info(s"An access policy will be created using $rlsCreateStatement")
      rlsCreateStatement
    }

    val rlsDeleteStatement = cliConfig.rls.map(_ => revokeAllPrivileges()).toList

    rlsDeleteStatement ++ rlsCreateStatements
  }

  val tableId: TableId = BigQueryJobBase.extractProjectDatasetAndTable(
    cliConfig.outputDataset + "." + cliConfig.outputTable
  )

  val datasetId: DatasetId = {
    scala.Option(tableId.getProject) match {
      case None =>
        DatasetId.of(projectId, cliConfig.outputDataset)
      case Some(project) =>
        DatasetId.of(project, tableId.getDataset)
    }
  }

  val bqTable = s"${cliConfig.outputDataset}.${cliConfig.outputTable}"

  def getOrCreateDataset(): Dataset = {
    val existingDataset = scala.Option(BigQueryJobBase.bigquery.getDataset(datasetId))
    val dataset = existingDataset.getOrElse {
      val datasetInfo = DatasetInfo
        .newBuilder(extractProjectDataset(cliConfig.outputDataset))
        .setLocation(cliConfig.getLocation())
        .build
      BigQueryJobBase.bigquery.create(datasetInfo)
    }
    setTagsOnDataset(dataset)
    dataset
  }

  protected def setTagsOnTable(table: Table): Unit = {
    cliConfig.starlakeSchema.foreach { schema =>
      val tableTagPairs = Utils.extractTags(schema.tags)
      table.toBuilder.setLabels(tableTagPairs.toMap.asJava).build().update()
    }
  }

  protected def setTagsOnDataset(dataset: Dataset): Unit = {
    cliConfig.domainTags.foreach { domainTags =>
      val datasetTagPairs = Utils.extractTags(Some(domainTags))
      dataset.toBuilder.setLabels(datasetTagPairs.toMap.asJava).build().update()
    }

  }

  /** To set access control on a table or view, we can use Identity and Access Management (IAM)
    * policy After you create a table or view, you can set its policy with a set-iam-policy call For
    * each call, we compare if the existing policy is equal to the defined one (in the Yaml file) If
    * it's the case, we do nothing, otherwise we update the Table policy
    * @param tableId
    * @param rls
    * @return
    */
  def applyACL(
    tableId: TableId,
    acl: List[AccessControlEntry]
  ): Policy = {
    // val BIG_QUERY_VIEWER_ROLE = "roles/bigquery.dataViewer"
    val existingPolicy: Policy = BigQueryJobBase.bigquery.getIamPolicy(tableId)
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
      BigQueryJobBase.bigquery.setIamPolicy(tableId, editedPolicy)
      editedPolicy
    } else {
      logger.info(s"Iam Policy is the same as before on this Table: $tableId")
      existingPolicy
    }
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

  val bigquery: BigQuery =
    if (sys.env.contains("GCLOUD_PROJECT"))
      BigQueryOptions.newBuilder().setProjectId(sys.env("GCLOUD_PROJECT")).build().getService
    else if (sys.env.contains("GOOGLE_CLOUD_PROJECT"))
      BigQueryOptions.newBuilder().setProjectId(sys.env("GOOGLE_CLOUD_PROJECT")).build().getService
    else
      BigQueryOptions.getDefaultInstance.getService

  // Lazy otherwise tests fail since there is no GCP credentials in test mode
  lazy val policyTagClient = PolicyTagManagerClient.create()

  def extractProjectDatasetAndTable(value: String): TableId = {
    def extractDatasetAndTable(str: String): (String, String) = {
      val sepIndex = str.indexOf('.')
      if (sepIndex > 0)
        (str.substring(0, sepIndex), str.substring(sepIndex + 1))
      else
        throw new Exception(s"Dataset cannot be null in BigQuery view name ($value)")
    }

    val sepIndex = value.indexOf(":")
    val (project, (dataset, table)) =
      if (sepIndex > 0)
        (Some(value.substring(0, sepIndex)), extractDatasetAndTable(value.substring(sepIndex + 1)))
      else // parquet is the default
        (None, extractDatasetAndTable(value))

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
