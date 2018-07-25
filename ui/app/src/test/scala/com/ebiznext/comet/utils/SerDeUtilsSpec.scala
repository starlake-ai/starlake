package com.ebiznext.comet.utils

import com.ebiznext.comet.model.CometModel.{ Cluster, Node, Tag, User }
import com.ebiznext.comet.utils.SerDeUtils.{ deserialize, serialize }
import org.scalatest.FlatSpec

/**
 * Created by Mourad on 23/07/2018.
 */
class SerDeUtilsSpec extends FlatSpec {

  "SerDeUtils" should "work on any Type" in {
    import SerDeUtils._
    val id: Int = 12345
    val key: String = "KEY1"
    val caseClass: Tag = Tag(id = "tagId", label = "tag1")

    assert(id == deserialize[Integer](serialize(Integer.valueOf(id))).get)
    assert(key == deserialize[String](serialize(key)).get)
    assert(caseClass == deserialize[Tag](serialize(caseClass)).get)

  }

  it should "work with multi level nested objects" in {
    val document: User = User("id", "mail@company.com",
      List(Cluster("c1", None, "",
        List(Node.empty.copy(id = "server1", tags =
          List(Tag("SSS", "SSS"), Tag("SSM", "SSM")))))))
    assert(document == deserialize[User](serialize(document)).get)

  }
}
