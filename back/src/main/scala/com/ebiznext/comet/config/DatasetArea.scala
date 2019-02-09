package com.ebiznext.comet.config

import com.ebiznext.comet.schema.handlers.StorageHandler
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.hadoop.fs.Path

/**
  * Utilities methods to reference datasets paths
  * Datasets paths are constructed as follows :
  *   - root path defined by the COMET_DATASETS env var or datasets applciation property
  *   - followed by the area name
  *   - followed by the the domain name
  */
object DatasetArea {

  def path(domain: String, area: String) =
    new Path(s"${Settings.comet.datasets}/$area/$domain")

  def path(domainPath: Path, schema: String) = new Path(domainPath, schema)

  /**
    * datasets waiting to be ingested are stored here
    *
    * @param domain : Domain Name
    * @return Absolute path to the pending folder of domain
    */
  def pending(domain: String): Path = path(domain, Settings.comet.area.pending)

  /**
    * datasets with a file name that could not match any schema file name pattern in the specified domain
    * are marked unresolved by being stored in this folder.
    *
    * @param domain : Domain name
    * @return Absolute path to the pending unresolved folder of domain
    */
  def unresolved(domain: String): Path =
    path(domain, Settings.comet.area.unresolved)

  /**
    * Once ingested datasets are archived in this folder.
    *
    * @param domain : Domain name
    * @return Absolute path to the archive folder of domain
    */
  def archive(domain: String): Path = path(domain, Settings.comet.area.archive)

  /**
    * Datasets of the specified domain currently being ingested are located in this folder
    *
    * @param domain : Domain name
    * @return Absolute path to the ingesting folder of domain
    */
  def ingesting(domain: String): Path =
    path(domain, Settings.comet.area.ingesting)

  /**
    * Valid records for datasets the specified domain are stored in this folder.
    *
    * @param domain : Domain name
    * @return Absolute path to the ingesting folder of domain
    */
  def accepted(domain: String): Path =
    path(domain, Settings.comet.area.accepted)

  /**
    * Invalid records and the reason why they have been rejected for the datasets of the specified domain are stored in this folder.
    *
    * @param domain : Domain name
    * @return Absolute path to the rejected folder of domain
    */
  def rejected(domain: String): Path =
    path(domain, Settings.comet.area.rejected)

  /**
    * Default target folder for autojobs applied to datasets in this domain
    *
    * @param domain : Domain name
    * @return Absolute path to the business folder of domain
    */
  def business(domain: String): Path =
    path(domain, Settings.comet.area.business)

  val metadata = new Path(s"${Settings.comet.metadata}")
  val types = new Path(metadata, "types")
  val domains = new Path(metadata, "domains")
  val jobs = new Path(metadata, "jobs")

  /**
    *
    * @param storage
    */
  def init(storage: StorageHandler): Unit = {
    List(metadata, types, domains).foreach(storage.mkdirs)
  }

  def initDomains(storage: StorageHandler, domains: Iterable[String]): Unit = {
    init(storage)
    domains.foreach { domain =>
      List(pending _, unresolved _, archive _, accepted _, rejected _, business _)
        .map(_(domain))
        .foreach(storage.mkdirs)
    }
  }
}

/**
  * After going through the data pipeline
  * a dataset may be referenced through a Hive table in a Hive Database.
  * For each input domain, 3 Hive databases may be created :
  *     - The rejected database : contains tables referencing rejected records for each schema in the domain
  *     - The accepted database : contains tables referencing
  *     - The business database : contains tables where autjob tables are created by default
  *     - The ciustom database : contains tables where autojob tables are created when a specific area is defined
  */
object HiveArea {

  def fromString(value: String): HiveArea = {
    value.toLowerCase() match {
      case Settings.comet.area.rejected => HiveArea.rejected
      case Settings.comet.area.accepted => HiveArea.accepted
      case Settings.comet.area.business => HiveArea.business
      case custom                       => HiveArea(custom)
    }
  }

  object rejected extends HiveArea(Settings.comet.area.rejected)

  object accepted extends HiveArea(Settings.comet.area.accepted)

  object business extends HiveArea(Settings.comet.area.business)

  def area(domain: String, area: HiveArea): String = s"${domain}_${area.value}"

}

class HiveAreaDeserializer extends JsonDeserializer[HiveArea] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): HiveArea = {
    val value = jp.readValueAs[String](classOf[String])
    HiveArea.fromString(value)
  }
}

@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[HiveAreaDeserializer])
sealed case class HiveArea(value: String) {
  override def toString: String = value
}
