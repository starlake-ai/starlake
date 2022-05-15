package ai.starlake.job.infer

import ai.starlake.TestHelper

class InferSchemaSpec extends TestHelper {
  new WithSettings {
    "All InferSchema Config" should "be known and taken  into account" in {
      val rendered = InferSchemaConfig.usage()
      val expected =
        """
          |Usage: starlake infer-schema [options]
          |
          |  --domain <value>       Domain Name
          |  --schema <value>       Schema Name
          |  --input <value>        Dataset Input Path
          |  --output <value>       Domain YAML Output Path
          |  --with-header <value>  Does the file contain a header (For CSV files only)
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }
}
