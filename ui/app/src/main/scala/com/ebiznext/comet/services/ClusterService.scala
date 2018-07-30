package com.ebiznext.comet.services
import java.nio.file.Path

import com.ebiznext.comet.model.CometModel.Cluster

import scala.concurrent.Future
import scala.util.Try

/**
  * Created by Mourad on 30/07/2018.
  */
class ClusterService {

  def create(userId: String, cluster: Cluster): Future[Try[String]] = ???

  def delete(userId: String, clusterId: String): Future[Unit] = ???

  def update(userId: String, clusterId: String, newCluster: Cluster): Future[Try[String]] = ???

  def clone(userId: String, clusterId: String, tagsOnly: Boolean): Future[Try[String]] = ???

  def buildAnsibleScript(userId: String, clusterId: String): Future[Try[Path]] = ???

}
