package com.ebiznext.comet.services

import com.ebiznext.comet.model.CometModel.{ Node, Tag, TagValue }

import scala.concurrent.Future

class TagService {

  def getTags(): Seq[Tag] = {
    Array[Tag]()
  }

  def buildTagIni(servers: Seq[Node]): String = {
    servers
      .map { server =>
        val section = s"[${server.name}]"
        val attrs = server.tags.map(tag => s"${tag.name}:${tag.value}").toList.mkString(";")
        s"""$section
         |DCOS_ATTRIBUTES=$attrs
       """.stripMargin
      }
      .toList
      .mkString("\n")
  }

  def createTag(userId: String, clusterId: String, tag: Tag): Future[Option[String]] = ???

  def deleteTag(userId: String, clusterId: String, tagName: String): Future[Unit] = ???

  def updateTag(userId: String, clusterId: String, tagName: String, newTag: Tag): Future[Unit] = ???

  def assignTagToNode(
    userId: String,
    clusterId: String,
    nodeName: String,
    tagName: String,
    tagValue: TagValue,
    useDefautValue: Boolean
  ): Future[Option[Node]] = ???

  def unassignTagFromNode(userId: String, clusterId: String, nodeName: String, tagName: String): Future[Option[Node]] =
    ???

}
