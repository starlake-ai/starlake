package com.ebiznext.comet.model

object CometModel {

  case class Tag(id: String, tags: Seq[String])
  case class Node(id: String)

}
