package com.ebiznext.comet.services
import java.nio.file.Path

import com.ebiznext.comet.db.RocksDBConnection
import com.ebiznext.comet.model.CometModel.{Cluster, User}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * Created by Mourad on 30/07/2018.
  */
class ClusterService(implicit executionContext: ExecutionContext, implicit val dbConnection: RocksDBConnection)
    extends LazyLogging {

  def get(userId: String, clusterId: String): Try[Option[Cluster]] = Try {
    val user: Option[User] = dbConnection.read[User](userId)
    user match {
      case Some(u) =>
        u.clusters.find(_.id == clusterId)
      case None =>
        val message = s"User with id $userId not found! "
        logger.error(message)
        throw new Exception(message)
    }
  }

  def create(userId: String, cluster: Cluster): Try[Option[String]] = Try {
    val user: Option[User] = dbConnection.read[User](userId)
    user match {
      case Some(u) =>
        val clusters: Set[Cluster] = u.clusters
        clusters.find(_.id == cluster.id) match {
          case Some(c) =>
            val message = s"Cluster object with id ${c.id} already exists!"
            logger.error(message)
            None

          case None =>
            val newUser = u.copy(id = u.id, clusters = clusters + cluster)
            dbConnection.write[User](newUser.id, newUser)
            Some(cluster.id)

        }
      case None =>
        val message = s"User with id $userId not found! "
        logger.error(message)
        throw new Exception(message)

    }
  }

  def delete(userId: String, clusterId: String): Try[Unit] = Try {
    val user: Option[User] = dbConnection.read[User](userId)
    user match {
      case Some(u) =>
        dbConnection.write[User](u.id, User(u.id, u.clusters.filterNot(_.id == clusterId)))
      case None =>
        val message = s"User with id $userId not found!"
        logger.error(message)
        throw new Exception(message)
    }
  }

  def update(userId: String, clusterId: String, newCluster: Cluster): Try[Option[Cluster]] = ???

  def clone(userId: String, clusterId: String, tagsOnly: Boolean): Try[Option[String]] = ???

  def buildAnsibleScript(userId: String, clusterId: String): Try[Option[Path]] = ???

}
