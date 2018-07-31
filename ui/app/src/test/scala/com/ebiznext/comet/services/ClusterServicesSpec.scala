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

  val clusterService: ClusterService = new ClusterService

  "ClusterService" when {
    "create" should {
      "return the new created cluster id" in {
        clusterService.create(user1.id, cluster1) match {
          case Failure(exception) => fail(exception)
          case Success(value)     => value shouldBe cluster1.id
        }
      }
    }
    "get" should {
      "return a cluster object by his id" in {
        clusterService.get(user1.id, cluster1.id) match {
          case Failure(exception) => fail(exception)
          case Success(value)     => value shouldBe cluster1
        }
      }
    }
    "delete" should {
      "return nothing neither the get deleted cluster id" in {
        clusterService.delete(user1.id, cluster1.id) match {
          case Failure(exception) => fail(exception)
          case Success(_)         => clusterService.get(user1.id, cluster1.id).toOption.isEmpty shouldBe true
        }
      }
    }
    "update" should {
      "return the newly updated cluster object" in {
        clusterService.update(user1.id, cluster1.id, newCluster1) match {
          case Failure(exception) => fail(exception)
          case Success(value)     => value shouldBe newCluster1
        }
      }
    }
    "clone" should {
      "return the id of the fully cloned cluster object" in {
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
      "return the id of the cloned cluster object that contains only the tags definitions" in {
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
