package com.ebiznext.comet.job.ingest

import java.util.regex.Pattern

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Try

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
      XmlSimplePrivacyJob.applyPrivacy(ds, attribute)
    }
    saveAccepted(acceptedPrivacyDF)
    (session.sparkContext.emptyRDD[Row], acceptedPrivacyDF.rdd)
  }

  override def name: String = "XML-SimplePrivacyJob"
}

object XmlSimplePrivacyJob {

  def applyPrivacy(
    inputDF: DataFrame,
    attribute: Attribute
  )(implicit settings: Settings): DataFrame = {
    implicit val encoder = inputDF.encoder
    val openTag = "<" + attribute.name + ">"
    val closeTag = "</" + attribute.name + ">"
    val pattern = Pattern.compile(s".*$openTag.*$closeTag.*")
    inputDF.mapPartitions { partition =>
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
  }
}
