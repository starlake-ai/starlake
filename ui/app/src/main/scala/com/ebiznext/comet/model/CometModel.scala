package com.ebiznext.comet.model
import com.ebiznext.comet.utils.JsonSupport

object CometModel extends JsonSupport {

  case class Tag(id: String, parity: String, min: String, max: String, name: String, color: String)
  case class Service(
    id: String,
    tag: Tag,
    podType: String,
    cpu: String,
    memory: String,
    count: String,
    dataLocation: String
  )
  case class Node(id: String, name: Option[String], ip: Option[String], services: Seq[Tag])
  case class Cluster(id: String, name: Option[String], inventoryFile: String, nodes: Seq[Node])
  case class User(id: String, mail: String, clusters: Seq[Cluster])

  object Node {
    def empty: Node = Node("", None, None, List())
  }

  object Tag {
    def empty: Tag = Tag("", "", "", "", "", "")
  }

}
