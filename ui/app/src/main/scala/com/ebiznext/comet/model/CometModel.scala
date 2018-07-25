package com.ebiznext.comet.model
import com.ebiznext.comet.utils.JsonSupport

object CometModel extends JsonSupport {

  case class Tag(id: String, label: String)
  case class Node(id: String, name: Option[String], ip: Option[String], tags: Seq[Tag])
  case class Cluster(id: String, name: Option[String], inventoryFile: String, nodes: Seq[Node])
  case class User(id: String, mail: String, clusters: Seq[Cluster])

  object Node {
    def empty: Node = Node("", None, None, List())
  }

}
