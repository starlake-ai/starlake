package com.ebiznext.comet.services
import com.ebiznext.comet.db.RocksDBConnectionMockBaseSpec
import org.scalatest.WordSpec

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

/**
 * Created by Mourad on 30/07/2018.
 */
class ClusterServicesSpec extends WordSpec with RocksDBConnectionMockBaseSpec {

  def awaitForResult[T](futureResult: Future[T]): T = Await.result(futureResult, 5.seconds)

  "ClusterService" when {
    "create" should {
      "return the new created cluster id" in {}
    }
    "delete" should {
      "return nothing neither the get deleted cluster id" in {}
    }
    "update" should {
      "return the newly updated cluster object" in {}
    }
    "clone" should {
      "return the id of the newly created cluster object" in {}
    }
    "buildAnsibleScript" should {
      "return the path of the generated zip on the server" in {}
    }
  }
}
