package com.ebiznext.comet.services

import com.ebiznext.comet.model.CometModel.{ Node, Tag, TagValue }

import scala.concurrent.Future

class TagService {

  def retrieve(userId: String, clusterId: String): Seq[Tag] = {
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

  def create(userId: String, clusterId: String, tag: Tag): Future[Option[String]] = ???

  def delete(userId: String, clusterId: String, tagName: String): Future[Unit] = ???

  def update(userId: String, clusterId: String, tagName: String, newTag: Tag): Future[Unit] = ???

}

object TagService extends TagService
