package com.ebiznext.comet.services
import com.ebiznext.comet.model.CometModel.Cluster

import scala.concurrent.Future

/**
 * Created by Mourad on 30/07/2018.
 */
class ClusterService {

  def createCluster(userId: String, cluster: Cluster): Future[Option[String]] = ???

  def deleteCluster(userId: String, clusterId: String): Future[Unit] = ???

  def updateCluster(userId: String, clusterId: String, newCluster: Cluster): Future[Cluster] = ???

  def cloneCluster(userId: String, clusterId: String, tagsOnly: Boolean): Future[Option[String]] = ???

  def buildAnsibleScript(userId: String, clusterId: String): Future[Option[String]] = ???

}
