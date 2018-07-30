package com.ebiznext.comet.services

import com.ebiznext.comet.model.CometModel.{Node, Tag, TagValue}

import scala.concurrent.Future
import scala.util.Try

class TagService {

  def getTags(): Seq[Tag] = {
    Array[Tag]()
  }

  def buildTagIni(servers: Seq[Node]): String = {
    servers
      .map { server =>
        val section = s"[${server.name}]"
        val attrs   = server.tags.map(tag => s"${tag.name}:${tag.value}").toList.mkString(";")
        s"""$section
         |DCOS_ATTRIBUTES=$attrs
       """.stripMargin
      }
      .toList
      .mkString("\n")
  }

  def create(userId: String, clusterId: String, tag: Tag): Future[Try[String]] = ???

  def delete(userId: String, clusterId: String, tagName: String): Future[Unit] = ???

  def update(userId: String, clusterId: String, tagName: String, newTag: Tag): Future[Unit] = ???

  def assignToNode(
      userId: String,
      clusterId: String,
      tagValue: TagValue,
      useDefautValue: Boolean
  ): Future[Try[Node]] = ???

  def unassignFromNode(userId: String, clusterId: String, nodeName: String, tagName: String): Future[Try[Node]] =
    ???

}
