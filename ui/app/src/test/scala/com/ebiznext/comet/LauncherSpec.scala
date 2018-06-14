package com.ebiznext.comet

import com.ebiznext.comet.model.CometModel.Tag
import com.ebiznext.comet.services.{NodeService, TagService}
import com.ebiznext.comet.utils.Launcher
import org.scalatest.{Matchers, WordSpec}

class LauncherSpec extends WordSpec with Matchers {
  "Ansible" should {
    "getNodes" in {
      val (exit, out, err)= Launcher.runCommand("ansible agent,master -i /Users/hayssams/git/datalab-v2/datalab/platforms/demo/ovh_inventory.ini --list-hosts")
      System.out.println(exit)
      System.out.println(out)
      System.out.println(err)
      new NodeService().getNodes().foreach(println)
      assert(true)
    }
    "BuildTagsIni" in {
      println(new TagService().buildTagIni(List(Tag("server1", List("SSS", "SSM")), Tag("server2", List("JKM", "JKS")))))
      assert(true)
    }
  }
}
