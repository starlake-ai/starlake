package com.ebiznext.comet.services

import com.ebiznext.comet.model.CometModel.{Node, Tag}

class TagService {

  def getTags(): Seq[Tag] = {
    Array[Tag]()
  }

  def buildTagIni(servers: Seq[Node]): String = {
    servers.map { server =>
      val section = s"[${server.name}]"
      val attrs = server.tags.map(tag => s"${tag.name}:${tag.value}").toList.mkString(";")
      s"""$section
         |DCOS_ATTRIBUTES=$attrs
       """.stripMargin
    }.toList.mkString("\n")
  }
}
