package com.ebiznext.comet

import com.ebiznext.comet.model.CometModel.{ Node, TagValue }
import com.ebiznext.comet.services.{ NodeService, TagService }
import com.ebiznext.comet.utils.Launcher
import org.scalatest.{ Matchers, WordSpec }

class LauncherSpec extends WordSpec with Matchers {
  "Ansible" should {
    "getNodes" in {
      val path = getClass.getResource("/inventory.ini").getPath
      val (exit, out, err) = Launcher.runCommand(s"ansible agent,master -i $path --list-hosts")
      assert(exit == 0)
      new NodeService().getNodes().foreach(println)
      assert(true)
    }
    "getGroups" in {
      val res = new NodeService().getGroups()
      println(res)
    }
    "BuildTagsIni" in {
      println(
        new TagService().buildTagIni(
          List(Node("Server1", Set(TagValue("SSS", "SSM"))), Node("Server2", Set(TagValue("JKM", "JKM"))))
        )
      )
      assert(true)
    }
  }
}
