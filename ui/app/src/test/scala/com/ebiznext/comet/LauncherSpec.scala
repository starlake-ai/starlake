package com.ebiznext.comet

import com.ebiznext.comet.model.CometModel.{Node, TagValue}
import com.ebiznext.comet.services.{NodeService, TagService}
import com.ebiznext.comet.utils.Launcher
import org.scalatest.{Matchers, WordSpec}

class LauncherSpec extends WordSpec with Matchers {
  val path = getClass.getResource("/inventory.ini").getPath
  "Ansible" should {
    "getNodes" in {
      val (exit, out, err) =
        Launcher.runCommand(s"ansible agent,master -i $path --list-hosts")
      assert(exit == 0)
      new NodeService().getNodes(path).foreach(println)
      assert(true)
    }
    "getGroups" in {
      val res = new NodeService().getGroups(path)
      println(res)
    }
    "BuildTagsIni" in {
      println(
        new TagService().buildTagIni(
          List(
            Node("Server1", Map("SSS" -> TagValue("SSS", "SSM", false))),
            Node("Server2", Map("JKM" -> TagValue("JKM", "JKM", false)))
          )
        )
      )
      assert(true)
    }
  }
}
