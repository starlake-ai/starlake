package com.ebiznext.comet.model

import java.awt.Color
import java.nio.file.Path

import com.ebiznext.comet.model.CometModel.Parity.IGNORE
import com.ebiznext.comet.utils.JsonSupport

object CometModel extends JsonSupport {

  sealed case class Parity(value: String)

  case class Tag(name: String,
                 defaultValue: String,
                 parity: Parity = IGNORE,
                 min: Int = 1,
                 max: Int = Integer.MAX_VALUE,
                 color: Color = Color.blue)

  case class TagValue(tagName: String, value: String, useDefault: Boolean)

  case class Node(name: String, tags: Map[String, TagValue] = Map())

  case class Cluster(name: String,
                     inventoryFile: Path,
                     tags: Map[String, Tag] = Map(),
                     nodes: Map[String, Node] = Map(),
                     nodeGroups: Map[String, NodeGroup] = Map())

  case class User(name: String, clusters: Map[String, Cluster] = Map())

  case class NodeGroup(name: String, nodes: Map[String, Node])

  object Parity {

    val values = Seq(ODD, EVEN, IGNORE)

    object ODD extends Parity("ODD")

    object EVEN extends Parity("EVEN")

    object IGNORE extends Parity("IGNORE")
  }

}
