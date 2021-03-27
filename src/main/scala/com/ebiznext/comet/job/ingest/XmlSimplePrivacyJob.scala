package com.ebiznext.comet.job.ingest

import java.util.regex.Pattern
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

/** Used only to apply data masking rules (privacy) on one or more simple elements in XML data.
  * The input XML file is read as a text file. Privacy rules are applied on the resulting DataFrame and the result is
  * saved accepted area.
  * In the definition of the XML Schema:
  * - schema.metadata.format should be set to TEXT_XML
  * - schema.attributes should only contain the attributes on which privacy should be applied
  * Comet.defaultWriteFormat should be set text in order to have an XML formatted output file
  * Comet.privacyOnly should be set to true to save the result in one file (coalesce 1)
  *
  * @param domain
  * @param schema
  * @param types
  * @param path
  * @param storageHandler
  * @param schemaHandler
  * @param options
  * @param settings
  */
class XmlSimplePrivacyJob(
  val domain: Domain,
  val schema: Schema,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler,
  val options: Map[String, String]
)(implicit val settings: Settings)
    extends IngestionJob {

  /** Dataset loading strategy (JSON / CSV / ...)
    *
    * @return Spark Dataframe loaded using metadata options
    */
  override protected def loadDataSet(): Try[DataFrame] = {
    Try {
      require(
        settings.comet.defaultWriteFormat == "text",
        "default-write-format should be set to text"
      )
      val df = session.read.text(path.map(_.toString): _*)
      df
    }
  }

  /** ingestion algorithm
    *
    * @param dataset
    */
  override protected def ingest(dataset: DataFrame): (RDD[_], RDD[_]) = {
    val privacyAttributes = schema.attributes.filter(_.getPrivacy() != PrivacyLevel.None)
    val acceptedPrivacyDF: DataFrame = privacyAttributes.foldLeft(dataset) { case (ds, attribute) =>
      XmlSimplePrivacyJob.applyPrivacy(ds, attribute, session)
    }
    saveAccepted(acceptedPrivacyDF)
    (session.sparkContext.emptyRDD[Row], acceptedPrivacyDF.rdd)
  }

  override def name: String = "XML-SimplePrivacyJob"
}

object XmlSimplePrivacyJob {

  def applyPrivacy(
    inputDF: DataFrame,
    attribute: Attribute,
    session: SparkSession
  )(implicit settings: Settings): DataFrame = {
    val openTag = "<" + attribute.name + ">"
    val closeTag = "</" + attribute.name + ">"
    val pattern = Pattern.compile(s".*$openTag.*$closeTag.*")
    val resultRDD: RDD[Row] = inputDF.rdd.mapPartitions { partition =>
      partition.map { row =>
        val line = row.getString(0)
        val privacy: String = pattern.matcher(line).matches() match {
          case false => line
          case true => {
            val openIndex = line.indexOf(openTag) + openTag.size
            val closeIndex = line.indexOf(closeTag)
            val prefix = line.substring(0, openIndex)
            val suffix = line.substring(closeIndex)
            val privacyInput = line.substring(openIndex, closeIndex)
            prefix + attribute.privacy.crypt(privacyInput, Map.empty) + suffix
          }
        }
        Row(privacy)
      }
    }
    val schema: StructType = StructType(Array(StructField("value", StringType)))
    session.createDataFrame(resultRDD, schema)
  }
}
