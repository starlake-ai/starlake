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
    Right("") should equal(r1)

    val r2 = CommentParser.stripComments("// a comment")
    Right("") should equal(r2)

    val r3 = CommentParser.stripComments("/* level1 /* level 2 */  */")
    Right("") should equal(r3)

    val r4 = CommentParser.stripComments("Text Before./* level1 /* level 2 */  */")
    Right("Text Before.") should equal(r4)

    val r5 = CommentParser.stripComments("/* level1 /* level 2 */  */Text after.")
    Right("Text after.") should equal(r5)

    val r6 = CommentParser.stripComments("Text Before./* level1 /* level 2 */  */Text after.")
    Right("Text Before.Text after.") should equal(r6)
  }
}
