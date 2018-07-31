package com.ebiznext.comet.services
import java.nio.file.Path

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.db.{RocksDBConfig, RocksDBConnection}
import com.ebiznext.comet.model.CometModel.Cluster
import com.softwaremill.macwire._

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * Created by Mourad on 30/07/2018.
  */
class ClusterService(implicit executionContext: ExecutionContext) {

  lazy val rocksDBConfig: RocksDBConfig    = Settings.rocksDBConfig
  lazy val dbConnection: RocksDBConnection = wire[RocksDBConnection]

  def create(userId: String, cluster: Cluster): Try[String] = ???

  def delete(userId: String, clusterId: String): Try[Unit] = ???

  def update(userId: String, clusterId: String, newCluster: Cluster): Try[Cluster] = ???

  def clone(userId: String, clusterId: String, tagsOnly: Boolean): Try[String] = ???

  def buildAnsibleScript(userId: String, clusterId: String): Try[Path] = ???

}
