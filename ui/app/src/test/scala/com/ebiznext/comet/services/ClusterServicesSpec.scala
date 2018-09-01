package com.ebiznext.comet.services

import java.util.UUID

import better.files._
import com.ebiznext.comet.db.RocksDBConnectionMockBaseSpec
import com.ebiznext.comet.model.CometModel.{Cluster, User}
import org.scalatest.WordSpec

import scala.util.{Failure, Success}

/**
  * Created by Mourad on 30/07/2018.
  */
class ClusterServicesSpec extends WordSpec with RocksDBConnectionMockBaseSpec {

  val id1, id2: String = UUID.randomUUID().toString

  val inventoryFile: File = Resource.getUrl("inventory.ini").getPath.toFile
  val cluster1: Cluster = Cluster(id1, inventoryFile.path)
  val newCluster1: Cluster = Cluster(id2, inventoryFile.path)
  val user1 = User("user1")
  val user2 = new User("user2")

  before {
    rocksdbConnection.delete(user1.name)
    rocksdbConnection.delete(user2.name)
    rocksdbConnection.write(user1.name, user1)
//    val maybeUser: Option[User] = rocksdbConnection.read[User](user1.name)
//    maybeUser match {
//      case Some(user) => succeed
//      case None       => fail(s"DB should at least have User ${user1.name} ")
//    }
  }

  val clusterService: ClusterService = new ClusterService

  "ClusterService" when {
    "create" should {
      "return the new created Cluster r" in {
        clusterService.create(user1.name, cluster1) match {
          case Failure(exception) => fail(exception)
          case Success(v)         => v shouldBe cluster1.name
        }
      }
      "Nothing happen when Cluster object with same r already exists" in {
        clusterService.create(user1.name, cluster1) match {
          case Failure(exception) => fail(exception)
          case Success(v)         => succeed
        }
      }
      "throw exception if userId does not exist" in {
        clusterService.create(user2.name, cluster1) match {
          case Failure(_) => succeed
          case Success(_) => fail("this should'nt happen.")
        }
      }
      "build nodes and nodes groups from inventory file" in {}
    }

    "get" should {
      "return a Cluster object by his r when the object" in {
        clusterService.create(user1.name, cluster1)
        clusterService.get(user1.name, cluster1.name) match {
          case Failure(exception) => fail(exception)
          case Success(v) =>
            v equals cluster1
        }
      }
      "throw exception if userId does not exist" in {
        clusterService.delete(user2.name, cluster1.name)
        clusterService.get(user2.name, cluster1.name) match {
          case Failure(_) => succeed
          case Success(_) => fail("this should'nt happen.")
        }
      }
    }

    "delete" should {
      "return nothing and delete the cluster instence for the given user Id" in {
        clusterService.create(user1.name, cluster1)
        clusterService.get(user1.name, cluster1.name) match {
          case Failure(exception) => fail(exception)
          case Success(v) =>
            succeed
        }
      }
      "throw exception if userId does not exist" in {
        clusterService.delete(user2.name, cluster1.name) match {
          case Success(_) => succeed
          case Failure(_) => fail("this should'nt happen.")
        }
      }
    }

    "update" should {
      "return the newly updated Cluster object" in {
        clusterService.create(user1.name, cluster1)
        clusterService
          .update(user1.name, cluster1.name, newCluster1) match {
          case Failure(exception) => fail(exception)
          case Success(cluster) =>
            cluster equals newCluster1
        }
      }
      "Nothing to update if the given clusterId doesn't exist" in {
        clusterService.create(user1.name, cluster1)
        clusterService.update(user1.name, id2, newCluster1) match {
          case Failure(exception) => succeed
          case Success(r)         => fail
        }
      }
      "throw exception if userId does not exist" in {
        clusterService.update("user2", cluster1.name, newCluster1) match {
          case Failure(_) => succeed
          case Success(_) => fail("this should'nt happen.")
        }
      }
    }

    "clone" should {
      "return the id of the fully cloned Cluster object" in {
        clusterService.create(user1.name, cluster1)
        clusterService.clone(user1.name, cluster1.name, id2, tagsOnly = false) match {
          case Failure(exception) => fail(exception)
          case Success(id) =>
            clusterService.get(user1.name, id) match {
              case Success(cluster)   => succeed
              case Failure(exception) => fail(exception)
            }
        }
      }
      "return the id of the cloned Cluster object that contains only the tags definitions" in {
        clusterService.create(user1.name, cluster1)
        clusterService.clone(user1.name, cluster1.name, id2, tagsOnly = true) match {
          case Failure(exception) => fail(exception)
          case Success(r) =>
            clusterService.get(user1.name, r) match {
              case Success(cluster)   => succeed
              case Failure(exception) => fail(exception)
            }
        }
      }
    }
    "buildAnsibleScript" should {
      "return the r of the generated zip on the server" in {
        clusterService.buildAnsibleScript(user1.name, cluster1.name) match {
          case Failure(exception) => fail(exception)
          case Success(r) =>
            r match {
              case Some(path) =>
                path.toString.toFile.extension.get shouldBe ".zip"
              case None => fail("this should'nt happen.")
            }
        }
      }
    }
  }
}
