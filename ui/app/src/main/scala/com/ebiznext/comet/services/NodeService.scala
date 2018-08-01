package com.ebiznext.comet.services

import com.ebiznext.comet.model.CometModel.{ Node, TagValue }
import com.ebiznext.comet.utils.Launcher
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.Future

class NodeService {
  def getNodes(): Seq[String] = {
    val (exit, out, err) = Launcher.runCommand("ansible all -i /Users/hayssams/git/datalab-v2/datalab/platforms/demo/ovh_inventory.ini --list-hosts")
    exit match {
      case 0 =>
        out.split("\n").map(_.trim).filter(!_.startsWith("hosts ("))
      case _ =>
        Nil
    }
  }

  def getGroups(): Map[String, List[String]] = {
    // "ansible localhost -i /Users/hayssams/git/datalab-v2/datalab/platforms/demo/ovh_inventory.ini -m debug -a 'var=groups'"
    val cmd = "ansible-playbook -i /Users/hayssams/git/datalab-v2/datalab/platforms/demo/ovh_inventory.ini  /Users/hayssams/git/comet/ui/app/src/main/resources/groups.yml"
    val (exit, out, err) = Launcher.runCommand(cmd)
    exit match {
      case 0 =>
        implicit val formats = org.json4s.DefaultFormats
        val start = "k: [localhost] => "
        val jsonStr: String = out.substring(out.indexOf(start) + start.length, out.lastIndexOf('}') + 1)
        println(jsonStr)
        val map = parse(jsonStr).extract[Map[String, Any]]
        val res = map("groups").asInstanceOf[Map[String, List[String]]]
        res.foreach(println)
        res
      case _ =>
        println(exit)
        println(out)
        println(err)
        Map()
    }
  }

  def assignTag(userId: String, clusterId: String, nodeName: String, tagValue: TagValue): Future[Option[Node]] = ???

  def unassignTag(userId: String, clusterId: String, nodeName: String, tagName: String): Future[Option[Node]] = ???
}

object NodeService extends NodeService
