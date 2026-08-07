package ai.starlake.semantic

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SemanticExportCmdSpec extends AnyFlatSpec with Matchers {

  "semantic-export parser" should "accept the lookml format and connection flag" in {
    val config = SemanticExportCmd.parse(
      Seq("--format", "lookml", "--connection", "analytics_wh", "--model", "ecommerce")
    )
    config shouldBe Some(
      SemanticExportConfig(
        format = "lookml",
        model = Some("ecommerce"),
        connection = Some("analytics_wh")
      )
    )
  }

  it should "keep accepting the ossie format and reject unknown formats" in {
    SemanticExportCmd.parse(Seq("--format", "ossie")) shouldBe Some(
      SemanticExportConfig(format = "ossie")
    )
    SemanticExportCmd.parse(Seq("--format", "tableau")) shouldBe None
  }
}
