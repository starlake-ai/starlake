package com.ebiznext.comet.services

import com.ebiznext.comet.model.CometModel.Tag

class TagService {
  def getTags(): Seq[Tag] = {
    Array[Tag]()
  }
  def buildTagIni(servers: Seq[Tag]) : String = {
    servers.map { server =>
      val section = s"[${server.id}]"
      val attrs = server.tags.map(tag => s"$tag:yes").toList.mkString(";")
      s"""$section
         |DCOS_ATTRIBUTES=$attrs
       """.stripMargin
    }.toList.mkString("\n")
  }
}
