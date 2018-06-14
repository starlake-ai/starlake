package com.ebiznext.comet

import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.ebiznext.comet.controllers.CometRoutes
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class CometRoutesSpec extends WordSpec with Matchers with ScalaFutures with ScalatestRouteTest
    with CometRoutes {

  lazy val routes = cometRoutes
  "UserRoutes" should {
    "return no users if no present (GET /users)" in {
      val request = HttpRequest(uri = "/tags")

      request ~> routes ~> check {
        status should ===(StatusCodes.OK)

        // we expect the response to be json:
        contentType should ===(ContentTypes.`application/json`)

        // and no entries should be in the list:
        entityAs[String] should ===("""{"tags":[]}""")
      }
    }
  }
//    "be able to add users (POST /tags)" in {
//      val ta = Tag("Kapi", 42, "jp")
//      val userEntity = Marshal(user).to[MessageEntity].futureValue // futureValue is from ScalaFutures
//
//      // using the RequestBuilding DSL:
//      val request = Post("/users").withEntity(userEntity)
//
//      request ~> routes ~> check {
//        status should ===(StatusCodes.Created)
//
//        // we expect the response to be json:
//        contentType should ===(ContentTypes.`application/json`)
//
//        // and we know what message we're expecting back:
//        entityAs[String] should ===("""{"description":"User Kapi created."}""")
//      }
//    }
//    //#testing-post
//
//    "be able to remove users (DELETE /users)" in {
//      // user the RequestBuilding DSL provided by ScalatestRouteSpec:
//      val request = Delete(uri = "/users/Kapi")
//
//      request ~> routes ~> check {
//        status should ===(StatusCodes.OK)
//
//        // we expect the response to be json:
//        contentType should ===(ContentTypes.`application/json`)
//
//        // and no entries should be in the list:
//        entityAs[String] should ===("""{"description":"User Kapi deleted."}""")
//      }
//    }
//    //#actual-test
//  }
}
//#set-up
//#user-routes-spec
