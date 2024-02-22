/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.config

import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import java.util.Locale

/** Utilities methods to reference datasets paths Datasets paths are constructed as follows :
  *   - root path defined by the SL_DATASETS env var or datasets application property
  *   - followed by the area name
  *   - followed by the the domain name
  */
object DatasetArea extends StrictLogging {

  def path(domain: String, area: String)(implicit settings: Settings): Path = {
    if (area.contains("://"))
      new Path(area)
    else if (settings.appConfig.datasets.contains("://"))
      new Path(
        s"${settings.appConfig.datasets}/$area/$domain"
      )
    else
      new Path(
        s"${settings.appConfig.fileSystem}/${settings.appConfig.datasets}/$area/$domain"
      )
  }

  def path(domain: String)(implicit settings: Settings): Path =
    if (settings.appConfig.datasets.contains("://"))
      new Path(
        s"${settings.appConfig.datasets}/$domain"
      )
    else
      new Path(
        s"${settings.appConfig.fileSystem}/${settings.appConfig.datasets}/$domain"
      )

  def path(domainPath: Path, schema: String) = new Path(domainPath, schema)

  /** datasets waiting to be ingested are stored here
    *
    * @param domain
    *   : Domain Name
    * @return
    *   Absolute path to the pending folder of domain
    */
  def pending(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.pending)

  /** datasets with a file name that could not match any schema file name pattern in the specified
    * domain are marked unresolved by being stored in this folder.
    *
    * @param domain
    *   : Domain name
    * @return
    *   Absolute path to the pending unresolved folder of domain
    */
  def unresolved(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.unresolved)

  /** Once ingested datasets are archived in this folder.
    *
    * @param domain
    *   : Domain name
    * @return
    *   Absolute path to the archive folder of domain
    */
  def archive(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.archive)

  /** Datasets of the specified domain currently being ingested are located in this folder
    *
    * @param domain
    *   : Domain name
    * @return
    *   Absolute path to the ingesting folder of domain
    */
  def ingesting(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.ingesting)

  /** Valid records for datasets the specified domain are stored in this folder.
    *
    * @param domain
    *   : Domain name
    * @return
    *   Absolute path to the ingesting folder of domain
    */
  def accepted(domain: String)(implicit settings: Settings): Path = {
    path(domain, settings.appConfig.area.accepted)
  }

  /** Invalid records and the reason why they have been rejected for the datasets of the specified
    * domain are stored in this folder.
    *
    * @param domain
    *   : Domain name
    * @return
    *   Absolute path to the rejected folder of domain
    */
  def rejected(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.rejected)

  def metrics(domain: String, schema: String)(implicit settings: Settings): Path =
    substituteDomainAndSchemaInPath(domain, schema, settings.appConfig.metrics.path)

  def audit(domain: String, schema: String)(implicit settings: Settings): Path =
    substituteDomainAndSchemaInPath(domain, schema, settings.appConfig.audit.path)

  def expectations(domain: String, schema: String)(implicit settings: Settings): Path =
    substituteDomainAndSchemaInPath(domain, schema, settings.appConfig.expectations.path)

  def replay(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.replay)

  def substituteDomainAndSchemaInPath(
    domain: String,
    schema: String,
    path: String
  ): Path = {
    new Path(
      substituteDomainAndSchema(domain, schema, path)
    )
  }

  def substituteDomainAndSchema(domain: String, schema: String, template: String): String = {
    val normalizedDomainName = Utils.keepAlphaNum(domain)
    template
      .replace("{{domain}}", domain)
      .replace("{{normalized_domain}}", normalizedDomainName)
      .replace("{{schema}}", schema)
  }

  def discreteMetrics(domain: String, schema: String)(implicit settings: Settings): Path =
    DatasetArea.metrics(domain, "discrete")

  def continuousMetrics(domain: String, schema: String)(implicit settings: Settings): Path =
    DatasetArea.metrics(domain, "continuous")

  def frequenciesMetrics(domain: String, schema: String)(implicit settings: Settings): Path =
    DatasetArea.metrics(domain, "frequencies")

  /** Default target folder for autojobs applied to datasets in this domain
    *
    * @param domain
    *   : Domain name
    * @return
    *   Absolute path to the business folder of domain
    */
  def business(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.business)

  def metadata(implicit settings: Settings): Path =
    new Path(s"${settings.appConfig.metadata}")

  def types(implicit settings: Settings): Path =
    new Path(metadata, "types")

  def dags(implicit settings: Settings): Path =
    new Path(settings.appConfig.dags)

  def expectations(implicit settings: Settings): Path =
    new Path(metadata, "expectations")

  def mapping(implicit settings: Settings): Path =
    new Path(metadata, "mapping")

  def load(implicit settings: Settings): Path =
    new Path(metadata, "load")

  def external(implicit settings: Settings): Path =
    new Path(metadata, "external")

  def extract(implicit settings: Settings): Path =
    new Path(metadata, "extract")

  def transform(implicit settings: Settings): Path =
    new Path(metadata, "transform")

  def iamPolicyTags()(implicit settings: Settings): Path =
    new Path(DatasetArea.metadata, "iam-policy-tags.sl.yml")

  /** @param storage
    */
  def initMetadata(
    storage: StorageHandler
  )(implicit settings: Settings): Unit = {
    List(metadata, types, load, external, extract, transform, expectations, mapping)
      .foreach(
        storage.mkdirs
      )

  }

  def initDomains(storage: StorageHandler, domains: Iterable[String])(implicit
    settings: Settings
  ): Unit = {
    domains.foreach { domain =>
      List(pending _, unresolved _, archive _, accepted _, rejected _, business _, replay _)
        .map(_(domain))
        .foreach(storage.mkdirs)
    }
  }
}

/** After going through the data pipeline a dataset may be referenced through a Hive table in a Hive
  * Database. For each input domain, 3 Hive databases may be created :
  *   - The rejected database : contains tables referencing rejected records for each schema in the
  *     domain
  *   - The accepted database : contains tables referencing
  *   - The business database : contains tables where autjob tables are created by default
  *   - The ciustom database : contains tables where autojob tables are created when a specific area
  *     is defined
  */
object StorageArea {

  def fromString(value: String)(implicit settings: Settings): StorageArea = {

    val lcValue = value.toLowerCase(Locale.ROOT)

    lcValue match {
      case _ if lcValue == settings.appConfig.area.rejectedFinal => StorageArea.rejected
      case _ if lcValue == settings.appConfig.area.acceptedFinal => StorageArea.accepted
      case _ if lcValue == settings.appConfig.area.replayFinal   => StorageArea.replay
      case _ if lcValue == settings.appConfig.area.businessFinal => StorageArea.business
      case custom                                                => StorageArea.Custom(custom)
    }
  }

  case object rejected extends StorageArea {
    def value: String = "rejected"
  }

  case object accepted extends StorageArea {
    def value: String = "accepted"
  }

  case object replay extends StorageArea {
    def value: String = "replay"
  }

  case object business extends StorageArea {
    def value: String = "business"
  }

  final case class Custom(value: String) extends StorageArea

  def area(domain: String, area: Option[StorageArea])(implicit settings: Settings): String =
    settings.appConfig.area.hiveDatabase.richFormat(
      Map.empty,
      Map(
        "domain" -> domain,
        "area"   -> area.map(_.toString).getOrElse("")
      )
    )
}

sealed abstract class StorageArea {
  def value: String
  override def toString: String = value
}
