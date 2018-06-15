package com.ebiznext.comet.services

import com.ebiznext.comet.model.CometModel.{Node, Tag}
import com.ebiznext.comet.utils.Launcher

class NodeService {
  def getNodes() : Seq[String] = {
    val (exit, out, err) = Launcher.runCommand("ansible agent,master -i /Users/hayssams/git/datalab-v2/datalab/platforms/demo/ovh_inventory.ini --list-hosts")
    exit match {
      case 0 =>
        out.split("\n").map(_.trim).filter(!_.startsWith("hosts ("))
      case _ =>
        Nil
    }
  }
}
