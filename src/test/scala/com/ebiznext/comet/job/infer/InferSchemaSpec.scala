package com.ebiznext.comet.job.infer

import com.ebiznext.comet.TestHelper

class InferSchemaSpec extends TestHelper {
  new WithSettings() {
    "All InferSchema Config" should "be known and taken  into account" in {
      val rendered = InferSchemaConfig.usage()
      val expected =
        """
          |Usage: comet [options]
          |
          |  --domain <value>       Domain Name
          |  --schema <value>       Domain Name
          |  --input <value>        Input Path
          |  --output <value>       Output Path
          |  --with-header <value>  Does the file contain a header
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }
}
