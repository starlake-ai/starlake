package com.ebiznext.comet.controllers

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.{get, post}
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.stream.scaladsl.FileIO
import akka.util.Timeout
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.model.CometModel.{Cluster, Tag, TagValue}
import com.ebiznext.comet.services.{ClusterService, NodeService, TagService}
import com.ebiznext.comet.utils.JsonSupport
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait CometRoutes extends JsonSupport {
  implicit def system: ActorSystem

  lazy val log = Logging(system, classOf[CometRoutes])
  implicit lazy val timeout = Timeout(5.seconds) // usually we'd obtain the timeout from the system's configuration
  lazy val cometRoutes: Route =
    pathPrefix("/api/nodes") {
      pathEnd {
        get {
          complete("")
        }
      }
    } ~
      pathPrefix("/api/services") {
        //#users-get-delete
        pathEnd {
          get {
            complete("")
          } ~
            post {
              complete("")
            }
        }
      } ~
      pathPrefix("clusters" / Segment) { clusterName =>
        pathEnd {
          post {
            extractRequestContext { ctx =>
              implicit val materializer = ctx.materializer
              fileUpload("inventory") {
                case (_, fileStream) =>
                  val targetPath = Paths.get(Settings.cometConfig.inventories) resolve userId
                  targetPath.toFile.delete()
                  val sink = FileIO.toPath(targetPath)
                  val writeResult = fileStream.runWith(sink)
                  onSuccess(writeResult) { result =>
                    result.status match {
                      case Success(_) =>
                        ClusterService.create(
                          userId,
                          Cluster(clusterName, targetPath)
                        )
                        complete(s"Successfully written ${result.count} bytes")
                      case Failure(e) => throw e
                    }
                  }
              }
            }
          } ~
            get {
              complete(ClusterService.get(userId, clusterName))
            } ~
            delete {
              complete(ClusterService.delete(userId, clusterName))
            }
        } ~
          pathPrefix("tags" / Segment) { tagName =>
            post {
              entity(as[Tag]) { tag =>
                complete(TagService.create(userId, clusterName, tag))
              }
            } ~
              get {
                complete(TagService.retrieve(userId, clusterName))
              } ~
              delete {
                complete(TagService.delete(userId, clusterName, tagName))
              } ~
              put {
                entity(as[Tag]) { tag =>
                  complete(TagService.update(userId, clusterName, tagName, tag))
                }
              }
          } ~
          pathPrefix("nodes" / Segment / "tags" / Segment) {
            (nodeName, tagName) =>
              post {
                entity(as[TagValue]) { tagValue =>
                  complete(
                    NodeService
                      .assignTag(userId, clusterName, nodeName, tagValue)
                  )
                }
              } ~
                delete {
                  complete(
                    NodeService
                      .unassignTag(userId, clusterName, nodeName, tagName)
                  )
                } ~
                put {
                  entity(as[Tag]) { tag =>
                    complete(
                      TagService.update(userId, clusterName, tagName, tag)
                    )
                  }
                }
          }
      } ~
      pathSingleSlash {
        if (webapp.length > 0)
          getFromFile(s"$webapp/index.html")
        else
          getFromResource("$webapp/index.html")
      } ~
      path(Remaining) { remaining: String =>
        if (webapp.length > 0)
          getFromFile(s"$webapp/$remaining")
        else
          getFromResource(s"webapp/$remaining")
      }
  val webapp = ConfigFactory.load().getString("webapp.root")
  val userId = ""
}
