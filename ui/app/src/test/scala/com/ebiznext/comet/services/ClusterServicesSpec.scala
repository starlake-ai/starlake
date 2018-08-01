package com.ebiznext.comet.services
import better.files._
import com.ebiznext.comet.db.RocksDBConnectionMockBaseSpec
import com.ebiznext.comet.model.CometModel.{Cluster, User}
import org.scalatest.WordSpec

import scala.util.{Failure, Success}

/**
  * Created by Mourad on 30/07/2018.
  */
class ClusterServicesSpec extends WordSpec with RocksDBConnectionMockBaseSpec {

  import scala.concurrent.ExecutionContext.Implicits.global

  val cluster1    = Cluster.empty.copy("id1", "inventoryFile1")
  val newCluster1 = Cluster.empty.copy("id1", "inventoryFile2")
  val user1       = User("user1", Set())

  before {
    rocksdbConnection.write(user1.id, user1)
    val maybeUser: Option[User] = rocksdbConnection.read[User](user1.id)
    maybeUser match {
      case Some(user) => succeed
      case None       => fail(s"DB should at least have User ${user1.id} ")
    }
  }

  val clusterService: ClusterService = new ClusterService

  "ClusterService" when {
    "create" should {
      "return the new created Cluster id" in {
        clusterService.create(user1.id, cluster1) match {
          case Failure(exception) => fail(exception)
          case Success(value)     => value shouldBe cluster1.id
        }
      }
      "throw exception if Cluster object with same id already exists" in {
        clusterService.create(user1.id, cluster1)
        clusterService.create(user1.id, cluster1) match {
          case Failure(exception) => succeed
          case Success(value)     => fail("this should'nt happen.")
        }
      }
      "throw exception if userId does not exist" in {
        clusterService.create("user2", cluster1) match {
          case Failure(exception) => succeed
          case Success(value)     => fail("this should'nt happen.")
        }
      }
    }

    "get" should {
      "return a Cluster object by his id when the object" in {
        clusterService.create(user1.id, cluster1)
        clusterService.get(user1.id, cluster1.id) match {
          case Failure(exception) => fail(exception)
          case Success(value)     => value shouldBe cluster1
        }
      }
      "throw an Exception when there is no ClusterID of the given userId" in {
        clusterService.create(user1.id, cluster1)
        clusterService.get(user1.id, "id2") match {
          case Failure(exception) => succeed
          case Success(value)     => fail("this should'nt happen.")
        }
      }
      "throw exception if userId does not exist" in {
        clusterService.get("user2", cluster1.id) match {
          case Failure(exception) => succeed
          case Success(value)     => fail("this should'nt happen.")
        }
      }
    }
    "delete" should {
      "return nothing and delete the cluster instence for the given user Id" in {
        clusterService.delete(user1.id, cluster1.id) match {
          case Failure(exception) => fail(exception)
          case Success(_)         => clusterService.get(user1.id, cluster1.id).toOption.isEmpty shouldBe true
        }
      }
      "throw exception if userId does not exist" in {
        clusterService.delete("user2", cluster1.id) match {
          case Failure(exception) => succeed
          case Success(value)     => fail("this should'nt happen.")
        }
      }
    }
    "update" should {
      "return the newly updated Cluster object" in {
        clusterService.update(user1.id, cluster1.id, newCluster1) match {
          case Failure(exception) => fail(exception)
          case Success(value)     => value shouldBe newCluster1
        }
      }
      "Nothing to update if the given clusterId doesn't exist" in {
        clusterService.create(user1.id, cluster1)
        clusterService.update(user1.id, "id2", newCluster1) match {
          case Failure(exception) => {
            clusterService.get(user1.id, "id2").toOption.isEmpty
          }
          case Success(value) => fail("this should'nt happen.")
        }
      }
      "throw exception if userId does not exist" in {
        clusterService.update("user2", cluster1.id, newCluster1) match {
          case Failure(exception) => succeed
          case Success(value)     => fail("this should'nt happen.")
        }
      }
    }
    "clone" should {
      "return the id of the fully cloned Cluster object" in {
        clusterService.clone(user1.id, cluster1.id, false) match {
          case Failure(exception) => fail(exception)
          case Success(id) => {
            clusterService.get(user1.id, id).toOption match {
              case Some(cluster) => {
                cluster.id shouldBe id
                cluster.id shouldNot be(cluster1.id)
                cluster.inventoryFile shouldBe cluster1.inventoryFile
                cluster.tags.filterNot(cluster1.tags).isEmpty shouldBe true
                cluster.nodeGroups.filterNot(cluster1.nodeGroups).isEmpty shouldBe true
                cluster.nodes.filterNot(cluster1.nodes).isEmpty shouldBe true
              }
              case None => fail("We expect to have a Cluster instance!")
            }
          }
        }
      }
      "return the id of the cloned Cluster object that contains only the tags definitions" in {
        clusterService.clone(user1.id, cluster1.id, true) match {
          case Failure(exception) => fail(exception)
          case Success(id) => {
            clusterService.get(user1.id, id).toOption match {
              case Some(cluster) => {
                cluster.id shouldBe id
                cluster.id shouldNot be(cluster1.id)
                cluster.inventoryFile.isEmpty shouldBe true
                cluster.tags.filterNot(cluster1.tags).isEmpty shouldBe true
                cluster.nodeGroups.isEmpty shouldBe true
                cluster.nodes.isEmpty shouldBe true
              }
              case None => fail("We expect to have a Cluster instance!")
            }
          }
        }
      }
    }
    "buildAnsibleScript" should {
      "return the path of the generated zip on the server" in {
        clusterService.buildAnsibleScript(user1.id, cluster1.id) match {
          case Failure(exception) => fail(exception)
          case Success(path)      => path.toString.toFile.extension shouldBe "zip"
        }
      }
    }
  }
}
