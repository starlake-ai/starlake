package ai.starlake.job.gcp

import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  Field,
  PolicyTags,
  Schema,
  StandardTableDefinition,
  Table,
  TableId
}
import com.google.cloud.datacatalog.v1.{
  GetPolicyTagRequest,
  ListPolicyTagsRequest,
  ListTaxonomiesRequest,
  PolicyTagManagerClient
}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DatasetLogging
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

// https://cloud.google.com/data-catalog/docs/samples/data-catalog-ptm-list-policytags

class DataCatalogTest
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with StrictLogging
    with DatasetLogging {
  "Taxonomy" should "Work" in {
    if (sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
      val client = PolicyTagManagerClient.create()
      val taxonomyListRequest =
        ListTaxonomiesRequest
          .newBuilder()
          .setParent("projects/starlake-325712/locations/eu")
          .build()
      val taxonomyList = client.listTaxonomies(taxonomyListRequest)
      val taxonomyRef =
        taxonomyList.iterateAll().asScala.filter(_.getDisplayName() == "RGPD").map(_.getName).head
      val policyTagsRequest = ListPolicyTagsRequest.newBuilder().setParent(taxonomyRef).build()
      val policyTags = client.listPolicyTags(policyTagsRequest)
      val policyTagRef =
        policyTags.iterateAll().asScala.filter(_.getDisplayName() == "PHI").map(_.getName).head

      val getPolicyTagRequest =
        GetPolicyTagRequest.newBuilder().setName(policyTagRef.toString).build()
      val policyTag = client.getPolicyTag(getPolicyTagRequest)
      println(policyTag)

      val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService
      val tableId = TableId.of("audit", "audit")
      val table: Table = bigquery.getTable(tableId)
      val tableDefinition = table.getDefinition().asInstanceOf[StandardTableDefinition]
      val bqSchema = tableDefinition.getSchema()
      val bqFields = bqSchema.getFields.asScala.toList
      val head = bqFields.head
      val tail = bqFields.tail
      val newField = Field
        .newBuilder(head.getName, head.getType, head.getSubFields)
        .setPolicyTags(
          PolicyTags.newBuilder().setNames(List(policyTagRef).asJava).build()
        )
        .build()

      val updatedFields = newField :: tail
      table.toBuilder
        .setDefinition(StandardTableDefinition.of(Schema.of(updatedFields: _*)))
        .build()
        .update()
    }
  }
}
