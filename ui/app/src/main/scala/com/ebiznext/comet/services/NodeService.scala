package com.ebiznext.comet.services

import better.files.Resource
import com.ebiznext.comet.utils.Launcher
import com.typesafe.scalalogging.LazyLogging
import org.json4s._
import org.json4s.jackson.JsonMethods._

class NodeService extends LazyLogging {
  def getNodes(inventoryFilePath: String): Seq[String] = {
    val (exit, out, err) = Launcher.runCommand("ansible all -i " + inventoryFilePath + " --list-hosts")
    exit match {
      case 0 =>
        out.split("\n").map(_.trim).filter(!_.startsWith("hosts ("))
      case _ =>
        Nil
    }
  }

  def getGroups(inventoryFilePath: String): Map[String, List[String]] = {
    val groupsYml: String = Resource.getUrl("groups.yml").getPath
    val cmd               = "ansible-playbook -i " + inventoryFilePath + "  " + groupsYml
    val (exit, out, err)  = Launcher.runCommand(cmd)
    exit match {
      case 0 =>
        implicit val formats = org.json4s.DefaultFormats
        val start            = "k: [localhost] => "
        val jsonStr: String  = out.substring(out.indexOf(start) + start.length, out.lastIndexOf('}') + 1)
        logger.info(jsonStr)
        val map = parse(jsonStr).extract[Map[String, Any]]
        val res = map("groups").asInstanceOf[Map[String, List[String]]]
        res.foreach(m => logger.info(m.toString()))
        res
      case _ =>
        logger.info(s"$exit")
        logger.info(out)
        logger.info(err)
        Map()
    }
  }
}
