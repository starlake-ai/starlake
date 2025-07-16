package ai.starlake.job.ingest

import ai.starlake.config.{PrivacyLevels, Settings}
import ai.starlake.job.validator.{CheckValidityResult, SimpleRejectedRecord}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row, SparkSession}

import java.util.regex.Pattern
import scala.util.{Failure, Success, Try}

/** Used only to apply data masking rules (privacy) on one or more simple elements in XML data. The
  * input XML file is read as a text file. Privacy rules are applied on the resulting DataFrame and
  * the result is saved accepted area. In the definition of the XML Schema:
  *   - schema.metadata.format should be set to TEXT_XML
  *   - schema.attributes should only contain the attributes on which privacy should be applied
  *     Comet.defaultWriteFormat should be set text in order to have an XML formatted output file
  *     Comet.privacyOnly should be set to true to save the result in one file (coalesce 1)
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
  val domain: DomainInfo,
  val schema: SchemaInfo,
  val types: List[Type],
  val path: List[Path],
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler,
  val options: Map[String, String],
  val accessToken: Option[String],
  val test: Boolean
)(implicit val settings: Settings)
    extends IngestionJob {

  /** Dataset loading strategy (JSON / CSV / ...)
    *
    * @return
    *   Spark Dataframe loaded using metadata options
    */
  override def loadDataSet(): Try[DataFrame] = {
    Try {
      require(
        settings.appConfig.defaultWriteFormat == "text",
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
  override protected def ingest(
    dataset: DataFrame
  ): (Dataset[SimpleRejectedRecord], Dataset[Row]) = {
    val privacyAttributes = schema.attributes.filter(_.resolvePrivacy() != TransformInput.None)
    val acceptedPrivacyDF: DataFrame = privacyAttributes.foldLeft(dataset) { case (ds, attribute) =>
      XmlSimplePrivacyJob.applyPrivacy(ds, attribute, session).toDF()
    }
    import session.implicits._
    saveAccepted(
      CheckValidityResult(
        session.emptyDataset[SimpleRejectedRecord],
        session.emptyDataFrame,
        acceptedPrivacyDF
      )
    ) match {
      case Failure(exception) => throw exception
      case Success(rejectedRecordCount) =>
        (session.emptyDataset[SimpleRejectedRecord], acceptedPrivacyDF);
    }
  }

  override def name: String = "privacy-" + super.name

  override def defineOutputAsOriginalFormat(rejectedLines: DataFrame): DataFrameWriter[Row] =
    rejectedLines.write.format("text")
}

object XmlSimplePrivacyJob {

  def applyPrivacy(
    inputDF: DataFrame,
    attribute: TableAttribute,
    session: SparkSession
  )(implicit settings: Settings): Dataset[String] = {
    import session.implicits._
    val openTag = "<" + attribute.name + ">"
    val closeTag = "</" + attribute.name + ">"
    val pattern = Pattern.compile(s".*$openTag.*$closeTag.*")
    val allPrivacyLevels = PrivacyLevels.allPrivacyLevels(settings.appConfig.privacy.options)

    inputDF.map { row =>
      val line = row.getString(0)
      val privacy: String = if (pattern.matcher(line).matches()) {
        val openIndex = line.indexOf(openTag) + openTag.length
        val closeIndex = line.indexOf(closeTag)
        val prefix = line.substring(0, openIndex)
        val suffix = line.substring(closeIndex)
        val privacyInput = line.substring(openIndex, closeIndex)
        val attrPrivacy = attribute.resolvePrivacy()
        val ((privacyAlgo, privacyParams), _) = allPrivacyLevels(attrPrivacy.value)
        prefix + attrPrivacy.crypt(privacyInput, Map.empty, privacyAlgo, privacyParams) + suffix
      } else {
        line
      }
      privacy
    }
  }
}
