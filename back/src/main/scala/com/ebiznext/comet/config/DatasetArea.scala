package com.ebiznext.comet.config

import com.ebiznext.comet.schema.handlers.StorageHandler
import org.apache.hadoop.fs.Path

object DatasetArea {

  val env: String = Settings.comet.env

  def path(domain: String, area: String) = new Path(s"/$env/datasets/$area/$domain")

  def path(domainPath: Path, schema: String) = new Path(domainPath, schema)

  def pending(domain: String): Path = path(domain, "pending")

  def unresolved(domain: String): Path = path(domain, "unresolved")

  def staging(domain: String): Path = path(domain, "staging")

  def archive(domain: String): Path = {

    path(domain, "archive")

  }

  def ingesting(domain: String): Path = path(domain, "ingesting")

  def accepted(domain: String): Path = path(domain, "accepted")

  def rejected(domain: String): Path = path(domain, "rejected")

  def business(domain: String): Path = path(domain, "business")

  val metadata = new Path(s"/$env/metadata")
  val types = new Path(metadata, "types")
  val domains = new Path(metadata, "domains")
  val business = new Path(metadata, "business")


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
      storage.mkdirs(staging(domain))
      storage.mkdirs(accepted(domain))
      storage.mkdirs(rejected(domain))
    }
  }
}

sealed case class HiveArea(value: String)

object HiveArea {

  object rejected extends HiveArea("rejected")

  object accepted extends HiveArea("accepted")

  object business extends HiveArea("business")

  def area(domain: String, area: HiveArea) = s"${domain}_${area.value}"

}