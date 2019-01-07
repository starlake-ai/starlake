package com.ebiznext.comet.config

import com.ebiznext.comet.schema.handlers.StorageHandler
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.hadoop.fs.Path

object DatasetArea {


  def path(domain: String, area: String) = new Path(s"${Settings.comet.datasets}/$area/$domain")

  def path(domainPath: Path, schema: String) = new Path(domainPath, schema)

  def pending(domain: String): Path = path(domain, Settings.comet.area.pending)

  def unresolved(domain: String): Path = path(domain, Settings.comet.area.unresolved)

  def archive(domain: String): Path = path(domain, Settings.comet.area.archive)

  def ingesting(domain: String): Path = path(domain, Settings.comet.area.ingesting)

  def accepted(domain: String): Path = path(domain, Settings.comet.area.accepted)

  def rejected(domain: String): Path = path(domain, Settings.comet.area.rejected)

  def business(domain: String): Path = path(domain, Settings.comet.area.business)

  val metadata = new Path(s"${Settings.comet.metadata}")
  val types = new Path(metadata, "types")
  val domains = new Path(metadata, "domains")
  val jobs = new Path(metadata, "jobs")


  def init(storage: StorageHandler): Unit = {
    storage.mkdirs(this.metadata)
    storage.mkdirs(this.types)
    storage.mkdirs(this.domains)
  }

  def initDomains(storage: StorageHandler, domains: Iterable[String]): Unit = {
    init(storage)
    domains.foreach { domain =>
      storage.mkdirs(pending(domain))
      storage.mkdirs(unresolved(domain))
      storage.mkdirs(archive(domain))
      storage.mkdirs(accepted(domain))
      storage.mkdirs(rejected(domain))
    }
  }
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

object HiveArea {
  def fromString(value: String): HiveArea = {
    value.toLowerCase() match {
      case Settings.comet.area.rejected => HiveArea.rejected
      case Settings.comet.area.accepted => HiveArea.accepted
      case Settings.comet.area.business => HiveArea.business
      case custom => HiveArea(custom)
    }
  }

  object rejected extends HiveArea(Settings.comet.area.rejected)

  object accepted extends HiveArea(Settings.comet.area.accepted)

  object business extends HiveArea(Settings.comet.area.business)

  def area(domain: String, area: HiveArea): String = s"${domain}_${area.value}"

}