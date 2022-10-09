package ai.starlake.utils

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DatasetLogging
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CommentParserSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with StrictLogging
    with DatasetLogging {
  "Taxonomy" should "list files by modification_time and name" in {
    val r1 = CommentParser.stripComments("/* a comment */")
    "" should equal(r1)

    val r2 = CommentParser.stripComments("// a comment")
    "" should equal(r2)

    val r3 = CommentParser.stripComments("/* comment  */")
    "" should equal(r3)

    val r4 = CommentParser.stripComments("Text Before./* comment  */")
    "Text Before." should equal(r4)

    val r5 = CommentParser.stripComments("/* comment /  */Text after.")
    "Text after." should equal(r5)

    val r6 = CommentParser.stripComments("Text Before./* comment  */Text after.")
    "Text Before.Text after." should equal(r6)

    val r7 = CommentParser.stripComments("Text Before.-- comment\nText after.")
    "Text Before.\nText after." should equal(r7)

    val r8 = CommentParser.stripComments("Text Before.# comment\nText after.")
    "Text Before.\nText after." should equal(r8)
  }
}
